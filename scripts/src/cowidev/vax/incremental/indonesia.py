import requests

import pandas as pd

from cowidev.utils.clean.dates import localdate
from cowidev.vax.utils.incremental import enrich_data, increment


class Indonesia:
    location = "Indonesia"
    source_url_ref = "https://data.covid19.go.id/public/index.html"
    source_url = "https://data.covid19.go.id/public/api/pemeriksaan-vaksinasi.json"

    def read(self) -> pd.Series:
        data = requests.get(self.source_url).json()
        data = data["vaksinasi"]["total"]
        return pd.Series(
            {
                "people_vaccinated": data["jumlah_vaksinasi_1"],
                "people_fully_vaccinated": data["jumlah_vaksinasi_2"],
                "total_vaccinations": data["jumlah_vaksinasi_1"] + data["jumlah_vaksinasi_2"],
            }
        )

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "date", localdate("Asia/Jakarta"))

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinovac")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url_ref)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_date).pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

    def export(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    Indonesia().export()
