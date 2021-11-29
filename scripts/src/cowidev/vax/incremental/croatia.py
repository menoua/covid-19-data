from datetime import timedelta

import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.utils.web import request_json


def read(source: str) -> pd.Series:

    data = request_json(source)

    total_vaccinations = int(data["CijepljenjeBrUtrosenihDoza"])
    people_vaccinated = int(data["CijepljeniJednomDozom"])
    people_fully_vaccinated = int(data["CijepljeniDvijeDoze"])
    date = str((pd.to_datetime(data["Datum"]) - timedelta(days=1)).date())

    return pd.Series(
        data={
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": date,
        }
    )


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Croatia")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://www.koronavirus.hr")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main():
    source = "https://www.koronavirus.hr/data/stats_latest.json"
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
