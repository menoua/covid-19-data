import datetime
import json

import requests
import pandas as pd

from cowidev.vax.utils.files import export_metadata_age
from cowidev.utils.clean import clean_date_series
from cowidev.utils import paths


class Israel:

    location: str = "Israel"
    source_url: str = "https://datadashboardapi.health.gov.il/api/queries/vaccinated"
    source_url_ref: str = "https://datadashboard.health.gov.il/COVID-19/general"
    source_url_age: str = (
        "https://github.com/dancarmoz/israel_moh_covid_dashboard_data/raw/master/vaccinated_by_age.csv"
    )

    def read(self) -> pd.DataFrame:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:85.0) Gecko/20100101 Firefox/85.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        }
        data = json.loads(requests.get(self.source_url, headers=headers).content)
        return pd.DataFrame.from_records(data)

    def read_age(self):
        return pd.read_csv(self.source_url_age)

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "Day_Date": "date",
                "vaccinated_cum": "people_vaccinated",
                "vaccinated_seconde_dose_cum": "people_fully_vaccinated",
                "vaccinated_third_dose_cum": "total_boosters",
            }
        )

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=df.date.str.slice(0, 10))

    def pipe_filter_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df.date < str(datetime.date.today())]

    def pipe_select_min_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.groupby(["people_vaccinated", "people_fully_vaccinated"], as_index=False).min()

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated + df.total_boosters)

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
        )

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_ref)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            if date >= "2021-01-07":
                return "Moderna, Pfizer/BioNTech"
            return "Pfizer/BioNTech"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipe_nulls_as_nans(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(people_fully_vaccinated=df.people_fully_vaccinated.replace(0, pd.NA))

    def pipe_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[
            [
                "date",
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
                "total_boosters",
                "location",
                "source_url",
                "vaccine",
            ]
        ]
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_date)
            .pipe(self.pipe_filter_date)
            .pipe(self.pipe_select_min_date)
            .pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_location)
            .pipe(self.pipe_source)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_nulls_as_nans)
            .pipe(self.pipe_output_columns)
        )

    def pipeline_age(self, df):
        # Melt
        df = df.melt("Date")
        # Separate age group and variable
        var = df.variable.str.extract(r"(\d+)[\+\-](\d*)\s(.+)")
        # Assign new columns and clean date
        df = df.assign(
            age_group_min=var[0],
            age_group_max=var[1],
            variable=var[2],
            date=clean_date_series(df.Date, "%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        # Keep last entry for each date
        df = df.sort_values("Date")
        df = df.drop_duplicates(subset=["date", "variable", "age_group_min", "age_group_max"], keep="last")
        df = df.drop(columns="Date")
        # Pivot and fix column names
        df = df.pivot(index=["date", "age_group_min", "age_group_max"], columns=["variable"], values=["value"])
        df.columns = [col[1] for col in df.columns]
        df = df.reset_index()
        # Ignore agr group 10-19
        df = df[(df.age_group_min != "10") | (df.age_group_max != "19")]
        # Final column creations
        df = df.assign(
            location="Israel",
            people_vaccinated_per_hundred=100 * df["first dose"] / df["pop"],
            people_fully_vaccinated_per_hundred=100 * df["second dose"] / df["pop"],
        )
        # Select output columns
        df = df[
            [
                "location",
                "date",
                "age_group_min",
                "age_group_max",
                "people_vaccinated_per_hundred",
                "people_fully_vaccinated_per_hundred",
            ]
        ]
        return df

    def export(self):
        destination = paths.out_vax(self.location)
        self.read().pipe(self.pipeline).to_csv(destination, index=False)
        # Export age data
        df_age = self.read_age().pipe(self.pipeline_age)
        df_age.to_csv(paths.out_vax(self.location, age=True), index=False)
        export_metadata_age(
            df_age,
            "Ministry of Health via github.com/dancarmoz/israel_moh_covid_dashboard_data",
            self.source_url_age,
        )


def main():
    Israel().export()
