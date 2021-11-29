import re
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.get(source)
        time.sleep(5)

        total_vaccinations = clean_count(driver.find_element_by_id("counter1").text)
        # people_vaccinated_share = driver.find_element_by_id("counter4").text
        # assert "One dose" in people_vaccinated_share
        # people_fully_vaccinated_share = driver.find_element_by_id("counter4a").text
        # assert "Two doses" in people_fully_vaccinated_share

    # This logic is only valid as long as Qatar *exclusively* uses 2-dose vaccines
    # people_vaccinated_share = float(re.search(r"[\d.]+", people_vaccinated_share).group(0))
    # people_fully_vaccinated_share = float(re.search(r"[\d.]+", people_fully_vaccinated_share).group(0))
    # vaccinated_proportion = people_vaccinated_share / (people_vaccinated_share + people_fully_vaccinated_share)
    # people_vaccinated = round(total_vaccinations * vaccinated_proportion)
    # people_fully_vaccinated = total_vaccinations - people_vaccinated

    date = localdate("Asia/Qatar")

    data = {
        "total_vaccinations": total_vaccinations,
        # "people_vaccinated": people_vaccinated,
        # "people_fully_vaccinated": people_fully_vaccinated,
        "date": date,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Qatar")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Moderna, Pfizer/BioNTech")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source, source)


def main():
    source = "https://covid19.moph.gov.qa/EN/Pages/Vaccination-Program-Data.aspx"
    data = read(source).pipe(pipeline, source)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        # people_vaccinated=data["people_vaccinated"],
        # people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )


if __name__ == "__main__":
    main()
