import time

import pandas as pd

from cowidev.utils.clean import clean_count, extract_clean_date
from cowidev.utils.web.scraping import get_driver
from cowidev.vax.utils.incremental import enrich_data, increment


class Philippines:
    def __init__(self) -> None:
        self.location = "Philippines"
        self.source_url = "https://e.infogram.com/_/yFVE69R1WlSdqY3aCsBF"
        self.source_url_ref = (
            "https://news.abs-cbn.com/spotlight/multimedia/infographic/03/23/21/philippines-covid-19-vaccine-tracker"
        )

    def read(self) -> pd.Series:
        return pd.Series(data=self._parse_data())

    def _parse_data(self) -> dict:
        with get_driver() as driver:
            driver.get(self.source_url)
            time.sleep(2)
            spans = [span for span in driver.find_elements_by_tag_name("span") if span.get_attribute("data-text")]
            # Date
            date = extract_clean_date(
                spans[6].text.replace("Sept", "Sep"),
                "\(as of ([a-zA-Z]+)\.\s?(\d{1,2}), (20\d{2})\)",
                "%b %d %Y",
                lang="en",
            )
            # Metrics
            total_vaccinations = clean_count(spans[8].text)
            people_fully_vaccinated = clean_count(spans[15].text)
            total_boosters = clean_count(spans[18].text)
        if total_vaccinations < people_fully_vaccinated:
            raise ValueError(
                "Check values for:\n"
                f"total_vaccinations\t\t{total_vaccinations}\npeople_fully_vaccinated\t\t{people_fully_vaccinated}"
            )
        if total_vaccinations < total_boosters:
            raise ValueError(
                f"Check values for:\ntotal_vaccinations\t\t{total_vaccinations}\ntotal_boosters\t\t{total_boosters}"
            )
        if people_fully_vaccinated < total_boosters:
            raise ValueError(
                "Check values for:\n"
                f"people_fully_vaccinated\t\t{people_fully_vaccinated}\ntotal_boosters\t\t{total_boosters}"
            )
        return {
            "total_vaccinations": total_vaccinations,
            # "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "total_boosters": total_boosters,
            "date": date,
        }

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinovac, Sputnik V",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "source_url",
            self.source_url_ref,
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

    def export(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            # people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            total_boosters=data["total_boosters"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    Philippines().export()
