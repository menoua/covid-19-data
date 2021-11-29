import re

import pandas as pd

from cowidev.utils.clean import clean_count, extract_clean_date
from cowidev.utils.web.scraping import get_driver
from cowidev.vax.utils.incremental import enrich_data, increment


class UnitedArabEmirates:
    def __init__(self) -> None:
        self.location = "United Arab Emirates"
        self.source_url = "https://fcsc.gov.ae/en-us/Pages/Covid19/UAE-Covid-19-Updates.aspx"

    def read(self) -> pd.Series:
        return self._parse_data()

    def _parse_data(self) -> pd.Series:
        with get_driver() as driver:
            driver.get(self.source_url)
            elem = driver.find_element_by_class_name("total_vaccination")
            total_vaccinations = self._parse_total_vaccinations(elem)
            population = self._estimate_population(elem, total_vaccinations)
            return pd.Series(
                {
                    "total_vaccinations": total_vaccinations,
                    "people_vaccinated": self._parse_people_vaccinated(elem, population),
                    "people_fully_vaccinated": self._parse_people_fully_vaccinated(elem, population),
                    "date": self._parse_date(driver),
                }
            )

    def _parse_total_vaccinations(self, elem) -> pd.Series:
        text_total = elem.find_element_by_class_name("numbers").text
        regex_total = r"Total: ([\d\,]+)"
        total_vaccinations = clean_count(re.search(regex_total, text_total).group(1))
        return total_vaccinations

    def _estimate_population(self, elem, total_vaccinations) -> pd.Series:
        regex = r"([\d\.]+) per 100 people"
        share_total = self._parse_relative_metric(elem, "percentage", regex)
        return total_vaccinations / share_total

    def _parse_people_vaccinated(self, elem, population) -> pd.Series:
        regex = r"Percentage of population who received one dose \(of COVID-19 vaccine\)\s{1,2}([\d\.]+)%"
        share_vaccinated = self._parse_relative_metric(elem, "dose1pct", regex)
        return round(share_vaccinated * population) if share_vaccinated else None

    def _parse_people_fully_vaccinated(self, elem, population) -> pd.Series:
        regex = r"Percentage of population fully vaccinated \(against COVID-19\)\s{1,2}([\d\.]+)%"
        share_fully_vaccinated = self._parse_relative_metric(elem, "fullyVaccintedpct", regex)
        return round(share_fully_vaccinated * population) if share_fully_vaccinated else None

    def _parse_relative_metric(self, elem, class_name: str, regex: str):
        try:
            text = elem.find_element_by_class_name(class_name).text
            metric = float(re.search(regex, text).group(1)) / 100
            return metric
        except:
            return None

    def _parse_date(self, driver) -> pd.Series:
        text_date = driver.find_element_by_class_name("full_data_set").text
        regex_date = r"Time period: 29 January 2020 - (\d{2} [a-zA-Z]+ 202\d)"
        return extract_clean_date(text_date, regex_date, "%d %B %Y", lang="en")

    def pipe_calculate_boosters(self, ds: pd.Series) -> pd.Series:
        total_boosters = (
            ds.total_vaccinations - ds.people_vaccinated - ds.people_fully_vaccinated
            if ds.people_vaccinated and ds.people_fully_vaccinated
            else None
        )
        return enrich_data(ds, "total_boosters", total_boosters)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinopharm/Wuhan, Sputnik V",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds.pipe(self.pipe_calculate_boosters)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_source)
        )

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
    UnitedArabEmirates().export()
