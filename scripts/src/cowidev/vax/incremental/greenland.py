import datetime
import re

import pandas as pd

from cowidev.utils.clean import clean_count, extract_clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Greenland:
    location: str = "Greenland"
    source_url: str = "https://corona.nun.gl"
    regex = {"date": r".*Nutarterneqarpoq: (\d+. [a-zA-Z]+202\d)"}

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        data = self._parse_data(soup)
        return pd.Series(data=data)

    def _parse_data(self, soup) -> dict:
        return {**self._parse_data_metrics(soup), **self._parse_data_date(soup)}

    def _parse_data_metrics(self, soup) -> dict:
        counters = soup.find_all(class_="text-brand-blue")
        dose_1 = clean_count(re.search(r"Innuttaasut ([\d\.]+)", counters[1].parent.find_all("dd")[-1].text).group(1))
        dose_2 = clean_count(re.search(r"Innuttaasut ([\d\.]+)", counters[2].parent.find_all("dd")[-1].text).group(1))
        if dose_1 < dose_2:
            raise ValueError("dose_1 cannot be higher than dose_2")
        return {"people_vaccinated": dose_1, "people_fully_vaccinated": dose_2}

    def _parse_data_date(self, soup) -> dict:
        date = soup.find(class_="text-gray-500").text
        date = date.strip() + str(datetime.date.today().year)
        date = extract_clean_date(date, self.regex["date"], "%d. %B%Y", lang="en")
        return {"date": date}

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Moderna")

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_metrics(self, ds: pd.Series) -> pd.Series:
        total_vaccinations = ds["people_vaccinated"] + ds["people_fully_vaccinated"]
        return enrich_data(ds, "total_vaccinations", total_vaccinations)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source).pipe(self.pipe_metrics)

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
    Greenland().export()
