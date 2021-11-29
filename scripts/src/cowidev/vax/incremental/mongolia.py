import pandas as pd

from cowidev.utils.clean import clean_date
from cowidev.utils.web import request_json
from cowidev.vax.utils.incremental import enrich_data, increment


vaccine_mapping = {
    "AstraZeneca / БНЭУ 🤝 + КОВАКС 🤝": "Oxford/AstraZeneca",
    "Pfizer-BioNTech / КОВАКС 🤝 + ЯПОН 🤝": "Pfizer/BioNTech",
    "Pfizer-BioNTech / КОВАКС 🤝": "Pfizer/BioNTech",
    "Синофарм / БНХАУ 🤝+ 💵": "Sinopharm/Beijing",
    "Спутник V / ОХУ 💵": "Sputnik V",
    "Спутник V / ОХУ 🤝 + 💵": "Sputnik V",
}


def read(source: str) -> pd.Series:
    data = request_json(source, parser="html.parser")
    return parse_data(data)


def parse_data(data: dict) -> pd.Series:

    date = clean_date(data["updated"], "%Y/%m/%d")

    people_vaccinated = data["progress"]
    people_fully_vaccinated = data["completed"]

    return pd.Series(
        data={
            "date": date,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "vaccine": ", ".join(_get_vaccine_names(data, translate=True)),
        }
    )


def _get_vaccine_names(data: dict, translate: bool = False) -> list:
    vaccine_names = [v["name"] for v in data["vaccines"]]
    _check_vaccine_names(vaccine_names)
    if translate:
        return sorted([vaccine_mapping[v] for v in vaccine_names])
    else:
        return sorted(vaccine_names)


def _check_vaccine_names(vaccine_names: list):
    unknown_vaccines = set(vaccine_names).difference(vaccine_mapping.keys())
    if unknown_vaccines:
        raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds.people_vaccinated + ds.people_fully_vaccinated
    return enrich_data(ds, "total_vaccinations", total_vaccinations)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Mongolia")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://ikon.mn/")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(add_totals).pipe(enrich_location).pipe(enrich_source)


def main():
    source = "https://ikon.mn/api/json/vaccine"
    data = read(source).pipe(pipeline)
    increment(
        location=str(data["location"]),
        total_vaccinations=int(data["total_vaccinations"]),
        people_vaccinated=int(data["people_vaccinated"]),
        people_fully_vaccinated=int(data["people_fully_vaccinated"]),
        date=str(data["date"]),
        source_url=str(data["source_url"]),
        vaccine=str(data["vaccine"]),
    )


if __name__ == "__main__":
    main()
