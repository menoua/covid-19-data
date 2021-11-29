import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    data = requests.get(source).json()
    return pd.Series(
        {
            "total_vaccinations": clean_count(data["main"]["rows"][0]["vaccasecount"]),
            "people_vaccinated": clean_count(data["main"]["rows"][0]["vactotalcount"]),
            "people_fully_vaccinated": clean_count(data["main"]["rows"][0]["vac2count"]),
            "date": clean_date(data["main"]["rows"][0]["lastupdated"], fmt="%d/%m/%Y"),
        }
    )


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Macao")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Pfizer/BioNTech, Sinopharm/Beijing")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://www.ssm.gov.mo/apps1/PreventCOVID-19/en.aspx")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main():
    source = "https://www.ssm.gov.mo/apps1/apps/healthdeclaration/monitor/monitor.json"
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
