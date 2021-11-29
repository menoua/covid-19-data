import json

import pandas as pd

from cowidev.utils.web.scraping import get_soup
from cowidev.utils import paths


URL = "https://coronadashboard.government.nl/landelijk/vaccinaties"


def main():
    soup = get_soup(URL)
    script = soup.find("script", id="__NEXT_DATA__")
    data = json.loads(script.string)

    doses = (
        pd.DataFrame.from_records(data["props"]["pageProps"]["selectedNlData"]["vaccine_administered_total"]["values"])
        .rename(columns={"date_unix": "date", "estimated": "total_vaccinations"})
        .drop(columns=["reported", "date_of_insertion_unix"])
    )
    doses["date"] = pd.to_datetime(doses.date, unit="s").dt.date.astype(str)

    coverage = (
        pd.DataFrame.from_records(data["props"]["pageProps"]["selectedNlData"]["vaccine_coverage"]["values"])
        .rename(
            columns={
                "date_end_unix": "date",
                "fully_vaccinated": "people_fully_vaccinated",
                "partially_or_fully_vaccinated": "people_vaccinated",
            }
        )
        .drop(columns=["date_of_insertion_unix", "partially_vaccinated", "date_start_unix"])
    )
    coverage["date"] = pd.to_datetime(coverage.date, unit="s").dt.date.astype(str)

    df = (
        pd.merge(doses, coverage, on="date", how="outer", validate="one_to_one")
        .sort_values("date")
        .assign(
            location="Netherlands",
            source_url="https://coronadashboard.government.nl/landelijk/vaccinaties",
        )
        .pipe(enrich_vaccine_name)
    )

    df = df[
        (df.total_vaccinations >= df.people_vaccinated)
        | (df.total_vaccinations.isna())
        | (df.people_vaccinated.isna())
    ]

    df.to_csv(paths.out_vax("Netherlands"), index=False)


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(dt: str) -> str:
        if dt < "2021-01-18":
            return "Pfizer/BioNTech"
        elif "2021-01-18" <= dt < "2021-02-10":
            return "Moderna, Pfizer/BioNTech"
        elif "2021-02-10" <= dt < "2021-04-21":
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        elif "2021-04-21" <= dt:
            return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

    return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))


if __name__ == "__main__":
    main()
