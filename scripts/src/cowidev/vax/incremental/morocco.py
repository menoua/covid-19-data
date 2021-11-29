import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Morocco:

    location: str = "Morocco"
    source_url: str = "http://www.covidmaroc.ma/pages/Accueilfr.aspx"

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        return self._parse_data(soup)

    def _parse_data(self, soup: BeautifulSoup) -> pd.Series:
        data = pd.Series(dtype="int")
        spans = soup.find("table").find_all("span")
        data["people_vaccinated"] = int(re.sub(r"[^\d]", "", spans[-3].text))
        data["people_fully_vaccinated"] = int(re.sub(r"[^\d]", "", spans[-2].text))
        data["total_vaccinations"] = data["people_vaccinated"] + data["people_fully_vaccinated"]
        return data

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        date = localdate("Africa/Casablanca")
        return enrich_data(ds, "date", date)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Sinopharm/Beijing")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

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
    Morocco().export()
