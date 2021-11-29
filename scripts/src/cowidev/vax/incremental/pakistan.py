import re

import pandas as pd

from cowidev.utils.clean import clean_count
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Pakistan:
    def __init__(self) -> None:
        self.location = "Pakistan"
        self.source_url = "https://ncoc.gov.pk/covid-vaccination-en.php"

    def read(self):
        soup = get_soup(self.source_url)
        return pd.Series(data=self._parse_data(soup))

    def _parse_data(self, soup):
        counters = soup.find_all(class_="counter")
        people_vaccinated = clean_count(counters[0].text)
        people_fully_vaccinated = clean_count(counters[1].text)
        total_vaccinations = clean_count(counters[2].text)

        date = soup.find("span", id="last-update").text
        date = re.search(r"\d+.*202\d", date).group(0)
        date = str((pd.to_datetime(date) - pd.Timedelta(days=1)).date())

        data = {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": date,
            "source_url": self.source_url,
        }

        return data

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", "Pakistan")

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "CanSino, Covaxin, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinovac, Sputnik V",
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine)

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
    Pakistan().export()
