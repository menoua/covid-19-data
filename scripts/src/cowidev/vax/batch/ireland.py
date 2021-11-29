import pandas as pd

from cowidev.utils.web import request_json
from cowidev.vax.utils.files import load_query
from cowidev.utils import paths


def read(source_protocol: str, source_boosters: str) -> pd.DataFrame:
    params = load_query("ireland-metrics", to_str=False)

    protocol_data = request_json(source_protocol, params=params)
    protocol_data = parse_data_protocol(protocol_data)

    boosters_data = request_json(source_boosters, params=params)
    boosters_data = parse_data_boosters(boosters_data)

    return pd.merge(protocol_data, boosters_data, how="outer", on="date", validate="one_to_one")


def parse_data_protocol(data: dict) -> int:
    records = [
        {
            "date": x["attributes"]["VaccinationDate"],
            "dose_1": x["attributes"]["Dose1Cum"],
            "dose_2": x["attributes"]["Dose2Cum"],
            "single_dose": x["attributes"]["SingleDoseCum"],
            "people_vaccinated": x["attributes"]["PartiallyVacc"],
            "people_fully_vaccinated": x["attributes"]["FullyVacc"],
        }
        for x in data["features"]
    ]
    return pd.DataFrame.from_records(records)


def parse_data_boosters(data: dict) -> int:
    records = [
        {
            "date": x["attributes"]["VaccinationDate"],
            "immuno_doses": x["attributes"]["ImmunoDoseCum"],
            "additional_doses": x["attributes"]["AdditionalDoseCum"],
        }
        for x in data["features"]
    ]
    return pd.DataFrame.from_records(records)


def add_totals(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        total_vaccinations=df.dose_1 + df.dose_2 + df.single_dose + df.immuno_doses + df.additional_doses,
        total_boosters=df.immuno_doses + df.additional_doses,
    ).drop(columns=["dose_1", "dose_2", "single_dose", "immuno_doses", "additional_doses"])


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=pd.to_datetime(df.date, unit="ms").dt.date).drop_duplicates(subset=["date"], keep=False)


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Ireland")


def pipe_vaccine(df: pd.DataFrame) -> str:
    def _enrich_vaccine(date):
        if date >= "2021-05-06":
            return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        if date >= "2021-02-05":
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        return "Pfizer/BioNTech"

    return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))


def enrich_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return df.assign(source_url=source)


def pipeline(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return (
        df.fillna(0)
        .pipe(add_totals)
        .pipe(enrich_location)
        .pipe(enrich_source, source)
        .pipe(format_date)
        .pipe(pipe_vaccine)
        .sort_values("date")
    )


def main():
    source_ref = "https://covid19ireland-geohive.hub.arcgis.com/"
    source_protocol = "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/COVID19_Daily_Vaccination/FeatureServer/0/query"
    source_boosters = "https://services-eu1.arcgis.com/z6bHNio59iTqqSUY/arcgis/rest/services/COVID19_HSE_vaccine_booster_dose_daily/FeatureServer/0/query"
    destination = paths.out_vax("Ireland")
    read(source_protocol, source_boosters).pipe(pipeline, source_ref).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
