import requests

import pandas as pd

from cowidev.utils.clean import clean_date_series
from cowidev.utils import paths


class Greece:
    def __init__(self, source_url: str, source_url_ref: str, location: str, token: str):
        self.source_url = source_url
        self.source_url_ref = source_url_ref
        self.location = location
        self.token = token

    def read(self) -> pd.DataFrame:
        data = requests.get(self.source_url, headers={"Authorization": f"Token {self.token}"}).json()
        df = pd.DataFrame.from_records(data)
        return (
            df.rename(
                columns={
                    "referencedate": "date",
                    "totaldistinctpersons": "people_vaccinated",
                    "totaldose2": "people_fully_vaccinated",
                    "totaldose3": "total_boosters",
                    "totalvaccinations": "total_vaccinations",
                }
            )[["people_vaccinated", "people_fully_vaccinated", "total_boosters", "total_vaccinations", "date"]]
            .groupby("date")
            .sum()
            .reset_index()
        )

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date, "%Y-%m-%dT%H:%M:%S"))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url_ref,
        )

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine_name(date: str) -> str:
            if date < "2021-01-13":
                return "Pfizer/BioNTech"
            elif "2021-01-13" <= date < "2021-02-10":
                return "Moderna, Pfizer/BioNTech"
            elif "2021-02-10" <= date < "2021-04-28":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            elif "2021-04-28" <= date:
                return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_metadata).pipe(self.pipe_vaccine).pipe(self.pipe_date)

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)


def main():
    Greece(
        source_url="https://www.data.gov.gr/api/v1/query/mdg_emvolio?date_from=2020-12-28",
        source_url_ref="https://www.data.gov.gr/datasets/mdg_emvolio/",
        location="Greece",
        token="b1ef5949bebace574a0d7e58b5cdf4018353121e",
    ).export()


if __name__ == "__main__":
    main()
