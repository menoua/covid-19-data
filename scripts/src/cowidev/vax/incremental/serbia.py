import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Serbia:
    def __init__(self):
        self.location = "Serbia"
        self.source_url = "https://vakcinacija.gov.rs/"
        self.regex = {
            "metrics": r"Број доза: ([\d.]+) – прва доза ([\d.]+), друга доза ([\d.]+), трећа доза ([\d.]+)",
            "date": r"ажурирано .*",
        }

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        (
            total_vaccinations,
            people_vaccinated,
            people_fully_vaccinated,
            total_boosters,
        ) = self._parse_metrics(soup)
        return pd.Series(
            {
                "total_vaccinations": total_vaccinations,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "total_boosters": total_boosters,
                "source_url": self.source_url,
                "date": self._parse_date(soup),
            }
        )

    def _parse_metrics(self, soup: BeautifulSoup):
        match = re.search(self.regex["metrics"], soup.text)
        total_vaccinations = clean_count(match.group(1))
        people_vaccinated = clean_count(match.group(2))
        people_fully_vaccinated = clean_count(match.group(3))
        total_boosters = clean_count(match.group(4))
        return total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters

    def _parse_date(self, soup: BeautifulSoup) -> str:
        elems = soup.find_all("p")
        x = []
        for elem in elems:
            if elem.find(text=re.compile(self.regex["date"])):
                x.append(elem)
        if len(x) > 1:
            raise ValueError("Format of source has changed")
        date_str = clean_date(x[0].text.strip(), "ажурирано %d.%m.%Y")
        return date_str

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V",
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_vaccine).pipe(self.pipe_location)

    def to_csv(self):
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
    Serbia().to_csv()


if __name__ == "__main__":
    main()
