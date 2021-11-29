import re

import pandas as pd

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Bangladesh:
    location: str = "Bangladesh"
    source_url: str = "http://103.247.238.92/webportal/pages/covid19-vaccination-update.php"

    def read(self) -> pd.Series:

        soup = get_soup(self.source_url, timeout=30)

        people_vaccinated = clean_count(
            re.search(r"^[\d,]+", soup.find_all(class_="info-box-number")[2].text).group(0)
        )
        people_fully_vaccinated = clean_count(
            re.search(r"^[\d,]+", soup.find_all(class_="info-box-number")[3].text).group(0)
        )
        total_vaccinations = people_vaccinated + people_fully_vaccinated

        date = localdate("Asia/Dhaka")

        return pd.Series(
            data={
                "total_vaccinations": total_vaccinations,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "date": date,
            }
        )

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", "Bangladesh")

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

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
    Bangladesh().export()
