from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    soup = get_soup(source)
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:

    total_vaccinations = clean_count(soup.find(id="stats").find_all("span")[0].text)
    people_fully_vaccinated = clean_count(soup.find(id="stats").find_all("span")[1].text)

    data = {
        "total_vaccinations": total_vaccinations,
        "people_fully_vaccinated": people_fully_vaccinated,
    }
    return pd.Series(data=data)


def format_date(ds: pd.Series) -> pd.Series:
    date = localdate("Europe/Chisinau")
    return enrich_data(ds, "date", date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Moldova")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(
        ds,
        "vaccine",
        "Johnson&Johnson, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V",
    )


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://vaccinare.gov.md/")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(format_date).pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main():
    source = "https://vaccinare.gov.md/"
    data = read(source).pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )


if __name__ == "__main__":
    main()
