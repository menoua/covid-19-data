import time

import pandas as pd

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_driver
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:

    with get_driver() as driver:
        driver.get(source)
        time.sleep(10)

        for block in driver.find_elements_by_class_name("kpimetric"):
            if "1ste dosis" in block.text and "%" not in block.text:
                people_partly_vaccinated = clean_count(block.find_element_by_class_name("valueLabel").text)
            elif "2de dosis" in block.text and "%" not in block.text:
                people_fully_vaccinated = clean_count(block.find_element_by_class_name("valueLabel").text)

    people_vaccinated = people_partly_vaccinated + people_fully_vaccinated

    return pd.Series(
        data={
            "total_vaccinations": people_vaccinated + people_fully_vaccinated,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": set_date(),
        }
    )


def set_date() -> str:
    return localdate("America/Paramaribo")


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Suriname")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://laatjevaccineren.sr/")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main():
    source = "https://datastudio.google.com/embed/u/0/reporting/1a7548f9-83d0-4516-8fe6-cacec6a293c4/page/igSUC"
    data = read(source).pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )


if __name__ == "__main__":
    main()
