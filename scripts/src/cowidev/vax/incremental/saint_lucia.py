import pandas as pd

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:

    soup = get_soup(source)

    az_dose1 = clean_count(soup.find_all(class_="yellow")[0].text)
    az_dose2 = clean_count(soup.find_all(class_="yellow")[1].text)
    assert az_dose1 >= az_dose2
    pfizer_dose1 = clean_count(soup.find_all(class_="yellow")[2].text)
    pfizer_dose2 = clean_count(soup.find_all(class_="yellow")[3].text)
    assert pfizer_dose1 >= pfizer_dose2

    people_vaccinated = az_dose1 + pfizer_dose1
    people_fully_vaccinated = az_dose2 + pfizer_dose2
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = localdate("America/St_Lucia")

    data = {
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Saint Lucia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://www.covid19response.lc/")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main():
    source = "https://www.covid19response.lc/"
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
