import pandas as pd

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:

    soup = get_soup(source)

    counters = soup.find_all(class_="elementor-counter-number")
    assert len(counters) == 4, "New counter in dashboard?"

    total_vaccinations = clean_count(counters[0]["data-to-value"])
    first_doses = clean_count(counters[1]["data-to-value"])
    second_doses = clean_count(counters[2]["data-to-value"])
    unique_doses = clean_count(counters[3]["data-to-value"])

    people_vaccinated = first_doses + unique_doses
    people_fully_vaccinated = second_doses + unique_doses

    date = localdate("America/Jamaica")

    return pd.Series(
        data={
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": date,
        }
    )


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Jamaica")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Johnson&Johnson, Moderna, Pfizer/BioNTech, Oxford/AstraZeneca")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source, source)


def main():
    source = "https://vaccination.moh.gov.jm/"
    data = read(source).pipe(pipeline, source)
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
