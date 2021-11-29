import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class SouthAfrica:
    def __init__(self):
        self.location = "South Africa"
        self.source_url = "https://sacoronavirus.co.za/"

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        return self._parse_data(soup)

    def _parse_data(self, soup: BeautifulSoup) -> pd.Series:
        return pd.Series(
            data={
                "date": localdate("Africa/Johannesburg"),
                "total_vaccinations": self._parse_total_vaccinations(soup),
            }
        )

    def _parse_total_vaccinations(self, soup: BeautifulSoup) -> str:
        return clean_count(
            soup.find(class_="counter-box-content", string=re.compile("Vaccines Administered")).parent.find(
                class_="display-counter"
            )["data-value"]
        )

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Johnson&Johnson, Pfizer/BioNTech")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

    def export(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=str(data["location"]),
            total_vaccinations=int(data["total_vaccinations"]),
            date=str(data["date"]),
            source_url=str(data["source_url"]),
            vaccine=str(data["vaccine"]),
        )


def main():
    SouthAfrica().export()
