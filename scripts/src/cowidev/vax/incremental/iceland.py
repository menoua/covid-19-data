import re
import json

import pandas as pd

from cowidev.utils.clean import clean_count, clean_date_series
from cowidev.utils.web import get_soup
from cowidev.vax.utils.incremental import increment
from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.utils import paths


VACCINE_PROTOCOLS = {
    "Pfizer": 2,
    "Moderna": 2,
    "AstraZeneca": 2,
    "Janssen": 1,
}

VACCINE_MAPPING = {
    "Pfizer/BioNTech": "Pfizer/BioNTech",
    "Moderna": "Moderna",
    "Oxford/AstraZeneca": "Oxford/AstraZeneca",
    "Janssen": "Johnson&Johnson",
}


def main():

    url = "https://e.infogram.com/c3bc3569-c86d-48a7-9d4c-377928f102bf"
    soup = get_soup(url)

    for script in soup.find_all("script"):
        if "infographicData" in str(script):
            json_data = str(script).replace("<script>window.infographicData=", "").replace(";</script>", "")
            json_data = json.loads(json_data)
            break

    metric_entities = {
        "total_vaccinations": "7287c058-7921-4abc-a667-ce298827c969",
        "people_vaccinated": "8d14f33a-d482-4176-af55-71209314b07b",
        "people_fully_vaccinated": "16a69e30-01fd-4806-920c-436f8f29e9bf",
        "total_boosters": "209af2de-9927-4c51-a704-ddc85e28bab9",
    }
    data = {}

    for metric, entity in metric_entities.items():
        value = json_data["elements"]["content"]["content"]["entities"][entity]["props"]["chartData"]["data"][0][0][0]
        value = re.search(r'18px;">([\d\.]+)', value).group(1)
        value = clean_count(value)
        data[metric] = value

    date = json_data["updatedAt"][:10]

    increment(
        location="Iceland",
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        total_boosters=data["total_boosters"],
        date=date,
        source_url="https://www.covid.is/tolulegar-upplysingar-boluefni",
        vaccine=", ".join(sorted(VACCINE_MAPPING.values())),
    )

    # By manufacturer
    data = json_data["elements"]["content"]["content"]["entities"]["e329559c-c3cc-48e9-8b7b-1a5f87ea7ad3"]["props"][
        "chartData"
    ]["data"][0]
    df = pd.DataFrame(data[1:]).reset_index(drop=True)
    df.columns = ["date"] + data[0][1:]

    df = df.melt("date", var_name="vaccine", value_name="total_vaccinations")

    df["date"] = clean_date_series(df["date"], "%d.%m.%Y")
    df["total_vaccinations"] = pd.to_numeric(df["total_vaccinations"], errors="coerce").fillna(0)
    df["total_vaccinations"] = df.sort_values("date").groupby("vaccine", as_index=False)["total_vaccinations"].cumsum()
    df["location"] = "Iceland"

    assert set(df["vaccine"].unique()) == set(
        VACCINE_MAPPING.keys()
    ), f"Vaccines present in data: {df['vaccine'].unique()}"
    df = df.replace(VACCINE_MAPPING)

    df.to_csv(paths.out_vax("Iceland", manufacturer=True), index=False)
    export_metadata_manufacturer(df, "Ministry of Health", url)


if __name__ == "__main__":
    main()
