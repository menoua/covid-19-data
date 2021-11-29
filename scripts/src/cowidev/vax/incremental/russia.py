import re

import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.utils.web import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    soup = get_soup(source)

    text = soup.find("div", id="data").find("p").text

    date = re.search(r"На сегодня \(([\d\.]{8})\)", text).group(1)
    date = clean_date(date, "%d.%m.%y")

    people_vaccinated = re.search(
        r"([\d\s]+) чел\. \([\d\.]+% от населения[^)]*\) - привито хотя бы одним компонентом вакцины",
        text,
    ).group(1)
    people_vaccinated = clean_count(people_vaccinated)

    people_fully_vaccinated = re.search(
        r"([\d\s]+) чел\. \([\d\.]+% от населения,?[^)]*\) - полностью привито", text
    ).group(1)
    people_fully_vaccinated = clean_count(people_fully_vaccinated)

    total_vaccinations = re.search(r"([\d\s]+) шт\. - всего прививок сделано", text).group(1)
    total_vaccinations = clean_count(total_vaccinations)

    total_boosters = re.search(r"([\d\s]+) чел\. - прошли ревакцинацию", text).group(1)
    total_boosters = clean_count(total_boosters)

    return pd.Series(
        {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "total_boosters": total_boosters,
            "date": date,
        }
    )


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Russia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Sputnik V, EpiVacCorona")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://gogov.ru/articles/covid-v-stats")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main():
    source = "https://gogov.ru/articles/covid-v-stats"
    data = read(source).pipe(pipeline)
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


if __name__ == "__main__":
    main()
