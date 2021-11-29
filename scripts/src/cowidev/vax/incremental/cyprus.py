import re

import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    soup = get_soup(source)

    for block in soup.find(class_="main").find_all(class_="w3-center"):

        if block.find("p").text == "ΣΥΝΟΛΟ ΕΜΒΟΛΙΑΣΜΩΝ":
            total_vaccinations = clean_count(block.find_all("p")[1].text)
            date = re.search(r"[\d/]{8,10}", block.find_all("p")[2].text)
            date = clean_date(date.group(0), "%d/%m/%Y")

        if block.find("p").text == "ΣΥΝΟΛΟ 1ης ΔΟΣΗΣ":
            people_vaccinated = clean_count(block.find_all("p")[1].text)

        if block.find("p").text == "ΣΥΝΟΛΟ 2ης ΔΟΣΗΣ":
            people_fully_vaccinated = clean_count(block.find_all("p")[1].text)

        if block.find("p").text == "ΣΥΝΟΛΟ 3ης ΔΟΣΗΣ":
            total_boosters = clean_count(block.find_all("p")[1].text)

    data = {
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "total_boosters": total_boosters,
        "date": date,
        "source_url": source,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Cyprus")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Pfizer/BioNTech, Oxford/AstraZeneca, Moderna, Johnson&Johnson")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine)


def main():
    source = "https://www.moh.gov.cy/moh/moh.nsf/All/0EFA027144C9E54AC22586BE0032B2F5"
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
