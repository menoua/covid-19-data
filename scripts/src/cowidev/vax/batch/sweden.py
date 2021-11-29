import requests

import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.utils.utils import make_monotonic
from cowidev.utils import paths


class Sweden(object):
    def __init__(self):
        """Constructor."""
        self.source_url_daily = (
            "https://fohm.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/statistik-och-analyser/"
            "statistik-over-registrerade-vaccinationer-covid-19/"
        )
        self.source_url_weekly = (
            "https://fohm.maps.arcgis.com/sharing/rest/content/items/fc749115877443d29c2a49ea9eca77e9/data"
        )
        self.location = "Sweden"
        self.columns_rename = None

    def read(self) -> pd.DataFrame:
        daily = self._read_daily_data()
        weekly = self._read_weekly_data()
        weekly = weekly[weekly["date"] < daily["date"].min()]
        return pd.concat([daily, weekly]).sort_values("date").reset_index(drop=True)

    def pipe_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url_daily)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_vaccine).pipe(self.pipe_columns).pipe(self.pipe_out_columns).pipe(make_monotonic)

    def _read_weekly_data_doses(self, dfs) -> pd.DataFrame:
        """Read weekly data for number of vaccinations administered."""
        # DOSES
        df = dfs["Vaccinationer tidsserie"]
        # Filter rows and columns of interest
        df_doses = df.loc[df.Region == "| Sverige |", ["Vecka", "År", "Antal vaccinationer"]]
        df_doses = df_doses.rename(columns={"Antal vaccinationer": "total_vaccinations"})
        return df_doses

    def _read_weekly_data_people(self, dfs) -> pd.DataFrame:
        """Read weekly data for number of vaccinated people."""
        # PEOPLE VAX
        df = dfs["Vaccinerade tidsserie"]
        # Filter rows and columns of interest
        df_people = df.loc[df.Region == "| Sverige |", ["Vecka", "År", "Antal vaccinerade", "Vaccinationsstatus"]]
        # Pivot & rename columns
        cols_wrong = set(df_people.Vaccinationsstatus).difference({"Minst 1 dos", "Minst 2 doser"})
        if cols_wrong:
            raise ValueError(f"Unknown columns: {cols_wrong}")
        df_people = (
            df_people.pivot(index=["Vecka", "År"], columns="Vaccinationsstatus", values="Antal vaccinerade")
            .reset_index()
            .rename(
                columns={
                    "Minst 1 dos": "people_vaccinated",
                    "Minst 2 doser": "people_fully_vaccinated",
                }
            )
        )
        return df_people

    def _read_weekly_data(self) -> pd.DataFrame:
        """Read weekly data

        This data is loaded from an excel. It contains very clean (but sparse, i.e. weekly) data.
        """
        dfs = pd.read_excel(self.source_url_weekly, sheet_name=None)
        # Read data
        df_doses = self._read_weekly_data_doses(dfs)
        df_people = self._read_weekly_data_people(dfs)
        # Merge
        df = df_doses.merge(df_people, on=["År", "Vecka"])
        # Date
        ds = df["År"].astype(str) + "-W" + df["Vecka"].astype(str) + "+0"
        df["date"] = ds.apply(lambda x: clean_date(x, "%Y-W%W+%w"))
        # Prepare output
        df = df.drop(columns=["Vecka", "År"]).sort_values("date")
        print(df)
        return df

    def _read_daily_data(self) -> pd.DataFrame:
        """Read daily data (latest) from HTML page."""
        text = requests.get(self.source_url_daily, verify=False).content
        dfs = pd.read_html(text, encoding="utf-8")
        df = self._get_df_daily(dfs[1])
        df_doses = self._get_df_doses_daily(dfs[0])
        df = self._merge_tables_daily(df, df_doses)
        return df

    def _read_daily_data_age_split(self) -> pd.DataFrame:
        """Read daily data (latest) from HTML page with two tables.

        One table with adult numbers, the other one with teen numbers (12-15 yo)."""
        text = requests.get(self.source_url_daily, verify=False).content
        dfs = pd.read_html(text, encoding="utf-8")
        df_adults = self._get_df_daily(dfs[1])
        df_teens = self._get_df_teens_daily(dfs[2])
        df_doses = self._get_df_doses_daily(dfs[0])
        df = self._merge_tables_daily_split(df_adults, df_teens, df_doses)
        return df

    def _get_df_daily(self, df):
        return df.assign(
            people_vaccinated=df["Antal vaccinerademed minst 1 dos*"].apply(clean_count),
            people_fully_vaccinated=df["Antal vaccinerademed 2 doser"].apply(clean_count),
        )

    def _get_df_teens_daily(self, df):
        # People vaccinated < 16 yo
        # df_teens = df.pivot("Datum", "Status", "Antal vaccinerade födda 2003-2005").reset_index()
        return df.assign(
            people_vaccinated=df["Antal vaccinerade med minst 1 dos"].apply(clean_count),
            # people_fully_vaccinated=df_teens["2 doser"].apply(clean_count),
        )

    def _get_df_doses_daily(self, df):
        # Total vaccinations
        return df.assign(
            total_vaccinations=df["Antal vaccinationer"].apply(clean_count),
        )

    def _merge_tables_daily(self, df, df_doses):
        # Merge
        df = df.merge(df_doses, on="Datum").rename(
            columns={
                "Datum": "date",
            }
        )
        df[["people_vaccinated", "people_fully_vaccinated"]] = df[
            ["people_vaccinated", "people_fully_vaccinated"]
        ].astype("Int64")
        df = df[["date", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]]
        return df

    def _merge_tables_daily_split(self, df_adults, df_teens, df_doses):
        # Merge people metrics
        df = df_adults.merge(df_teens, on="Datum", how="left")
        df = df.assign(
            people_vaccinated=df.filter(regex="people_vaccinated_*").sum(axis=1),
            people_fully_vaccinated=df.filter(regex="people_fully_vaccinated_*").sum(axis=1),
        )
        # Merge
        df = df.merge(df_doses, on="Datum").rename(
            columns={
                "Datum": "date",
            }
        )
        df[["people_vaccinated", "people_fully_vaccinated"]] = df[
            ["people_vaccinated", "people_fully_vaccinated"]
        ].astype("Int64")
        df = df[["date", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]]
        return df

    def pipe_out_columns(self, df: pd.DataFrame):
        return df[
            [
                "date",
                "people_vaccinated",
                "people_fully_vaccinated",
                "total_vaccinations",
                "vaccine",
                "location",
                "source_url",
            ]
        ]

    def export(self):
        """Generalized."""
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)


def main():
    Sweden().export()
