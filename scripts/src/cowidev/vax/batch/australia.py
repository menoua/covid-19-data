import io
import requests

import pandas as pd

from cowidev.vax.utils.utils import make_monotonic
from cowidev.utils.clean import clean_date
from cowidev.utils import paths


class Australia:
    def __init__(self):
        self.source_url = "https://covidbaseau.com/"
        self.source_file = "https://covidbaseau.com/people-vaccinated.csv"
        self.location = "Australia"
        self.columns_rename = {
            "dose_1": "people_vaccinated",
            "dose_2": "people_fully_vaccinated",
            "dose_3": "total_boosters",
        }

    def read(self) -> pd.DataFrame:
        response = requests.get("https://covidbaseau.com/people-vaccinated.csv")
        return pd.read_csv(io.StringIO(response.content.decode()))

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(total_vaccinations=df.dose_1 + df.dose_2 + df.dose_3)

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.columns_rename)

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=df.date.apply(clean_date, fmt="%Y-%m-%d", minus_days=1))

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            if date >= "2021-03-07":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            return "Pfizer/BioNTech"

        return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_date)
            .pipe(self.pipe_metadata)
            .pipe(make_monotonic)
            .sort_values("date")
        )

    def to_csv(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)


def main():
    Australia().to_csv()
