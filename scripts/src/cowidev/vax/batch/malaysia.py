import pandas as pd

from cowidev.vax.utils.utils import build_vaccine_timeline
from cowidev.utils import paths


class Malaysia:
    def __init__(self) -> None:
        self.location = "Malaysia"
        self.source_url = "https://github.com/CITF-Malaysia/citf-public/raw/main/vaccination/vax_malaysia.csv"
        self.source_url_ref = "https://github.com/CITF-Malaysia/citf-public"
        self._vax_2d = [
            "pfizer",
            "astra",
            "sinovac",
            "sinopharm",
        ]
        self._vax_1d = [
            "cansino",
        ]

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.source_url)

    def pipe_check_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        expected_cols = 28
        if df.shape[1] > expected_cols:
            print(df.columns)
            raise Exception(
                f"More columns ({df.shape[1]}) than expected ({expected_cols}) are present. Check for new vaccines?"
            )
        return df

    def pipe_filter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        all_vaccines = self._vax_2d + self._vax_1d + ["date"]
        reg = "|".join(all_vaccines)
        columns_kept = df.filter(regex=reg).columns.tolist()
        df = df[columns_kept].rename(columns={"cansino": "cansino1"})
        return df

    def pipe_calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.melt(id_vars="date", var_name="vaccine", value_name="doses")
        df["dose_number"] = df.vaccine.str.extract(r"(\d+)$").astype(int)
        df["vaccine"] = df.vaccine.str.replace(r"(\d+)$", "", regex=True)

        df = df.pivot(index=["date", "vaccine"], columns="dose_number", values="doses").reset_index().fillna(0)

        # total_vaccinations
        df["total_vaccinations"] = df[1] + df[2] + df[3]

        # people_vaccinated
        df["people_vaccinated"] = df[1]

        # people_fully_vaccinated
        df.loc[df.vaccine.isin(self._vax_2d), "people_fully_vaccinated"] = df[2]
        df.loc[df.vaccine.isin(self._vax_1d), "people_fully_vaccinated"] = df[1]

        # total_boosters
        df.loc[df.vaccine.isin(self._vax_2d), "total_boosters"] = df[3]
        df.loc[df.vaccine.isin(self._vax_1d), "total_boosters"] = df[2] + df[3]

        df = (
            df[["date", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters"]]
            .groupby("date", as_index=False)
            .sum()
            .sort_values("date")
        )

        df[["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters"]] = (
            df[["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters"]]
            .cumsum()
            .astype(int)
        )

        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url_ref,
        )

    def pipe_columns_out(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            [
                "date",
                "people_vaccinated",
                "people_fully_vaccinated",
                "total_vaccinations",
                "total_boosters",
                "vaccine",
                "location",
                "source_url",
            ]
        ]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_filter_columns)
            .pipe(self.pipe_check_columns)
            .pipe(self.pipe_calculate_metrics)
            .pipe(
                build_vaccine_timeline,
                {
                    "Pfizer/BioNTech": "2021-02-24",
                    "Sinovac": "2021-03-03",
                    "Oxford/AstraZeneca": "2021-05-03",
                    "CanSino": "2021-05-09",
                    "Sinopharm/Beijing": "2021-09-18",
                },
            )
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_columns_out)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)


def main():
    Malaysia().export()
