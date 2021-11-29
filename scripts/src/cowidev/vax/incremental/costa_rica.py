import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class CostaRica:
    location: str = "Costa Rica"
    source_url: str = "https://www.ccss.sa.cr/web/coronavirus/vacunacion"

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        return self._parse_data(soup)

    def _parse_data(self, soup: BeautifulSoup) -> pd.Series:
        total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters = self._parse_table(soup)
        assert total_vaccinations >= people_vaccinated >= people_fully_vaccinated
        assert total_vaccinations >= total_boosters

        data = {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "total_boosters": total_boosters,
            "date": self._parse_date(soup),
        }
        return pd.Series(data=data)

    def _parse_table(self, soup):
        df = pd.read_html(str(soup.find("table", id="content-table3")))[0]
        df = df[df["Región"] == "Total"]
        total_vaccinations = clean_count(df["Total dosis"].item())
        people_vaccinated = clean_count(df["Dosis 1"].item())
        people_fully_vaccinated = clean_count(df["Dosis 2"].item())
        total_boosters = clean_count(df["Dosis 3"].item())
        return total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters

    def _parse_date(self, soup):
        date = soup.find(class_="actualiza").text
        date = re.search(r"\d{2}-\d{2}-\d{4}", date).group(0)
        date = clean_date(date, "%d-%m-%Y")
        return date

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech")

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
            total_boosters=data["total_boosters"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    CostaRica().export()
