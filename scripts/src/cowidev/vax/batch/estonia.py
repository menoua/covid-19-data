import pandas as pd
from cowidev.utils import paths


def read(source: str) -> pd.DataFrame:
    return parse_data(source)


def parse_data(source: str) -> pd.DataFrame:
    df = pd.read_json(source)
    df = (
        df[["StatisticsDate", "MeasurementType", "TotalCount"]]
        .pivot(index="StatisticsDate", columns="MeasurementType", values="TotalCount")
        .reset_index()
        .rename(
            columns={
                "StatisticsDate": "date",
                "DosesAdministered": "total_vaccinations",
                "FullyVaccinated": "people_fully_vaccinated",
                "Vaccinated": "people_vaccinated",
            }
        )
    )
    return df[["date", "people_fully_vaccinated", "people_vaccinated", "total_vaccinations"]]


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Estonia")


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(date: str) -> str:
        if date < "2021-01-14":
            return "Pfizer/BioNTech"
        elif "2021-01-14" <= date < "2021-02-09":
            return "Moderna, Pfizer/BioNTech"
        elif "2021-02-09" <= date < "2021-04-14":
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        elif "2021-04-14" <= date:
            # https://vaktsineeri.ee/covid-19/vaktsineerimine-eestis/
            # https://vaktsineeri.ee/uudised/sel-nadalal-alustatakse-lamavate-haigete-ja-liikumisraskustega-inimeste-kodus-vaktsineerimist/
            return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

    return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))


def enrich_source(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(source_url="https://opendata.digilugu.ee")


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return df.pipe(enrich_location).pipe(enrich_vaccine_name).pipe(enrich_source)


def main():
    source = "https://opendata.digilugu.ee/covid19/vaccination/v2/opendata_covid19_vaccination_total.json"
    destination = paths.out_vax("Estonia")
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
