import pandas as pd

from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.utils import paths


def main():

    vaccine_mapping = {
        1: "Pfizer/BioNTech",
        2: "Moderna",
        3: "Oxford/AstraZeneca",
        4: "Johnson&Johnson",
    }
    one_dose_vaccines = ["Johnson&Johnson"]

    source = "https://www.data.gouv.fr/fr/datasets/r/b273cf3b-e9de-437c-af55-eda5979e92fc"

    df = pd.read_csv(source, usecols=["vaccin", "jour", "n_cum_dose1", "n_cum_dose2", "n_cum_dose3"], sep=";")

    df = df.rename(
        columns={
            "vaccin": "vaccine",
            "jour": "date",
            "n_cum_dose1": "people_vaccinated",
            "n_cum_dose2": "people_fully_vaccinated",
            "n_cum_dose3": "total_boosters",
        }
    )

    # Map vaccine names
    df = df[(df.vaccine.isin(vaccine_mapping.keys())) & (df.people_vaccinated > 0)]
    assert set(df["vaccine"].unique()) == set(vaccine_mapping.keys())
    df["vaccine"] = df.vaccine.replace(vaccine_mapping)

    # Add total doses
    df["total_vaccinations"] = df.people_vaccinated + df.people_fully_vaccinated + df.total_boosters

    manufacturer = df[["date", "total_vaccinations", "vaccine"]].assign(location="France")
    manufacturer.to_csv(paths.out_vax("France", manufacturer=True), index=False)
    export_metadata_manufacturer(manufacturer, "Public Health France", source)

    # Infer fully vaccinated for one-dose vaccines
    df.loc[df.vaccine.isin(one_dose_vaccines), "people_fully_vaccinated"] = df.people_vaccinated

    df = df.groupby("date", as_index=False).agg(
        {
            "total_vaccinations": "sum",
            "people_vaccinated": "sum",
            "people_fully_vaccinated": "sum",
            "total_boosters": "sum",
            "vaccine": lambda x: ", ".join(sorted(x)),
        }
    )

    df = df.assign(
        location="France",
        source_url=(
            "https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-personnes-vaccinees-contre-la-covid-19-1/"
        ),
    )

    df.to_csv(paths.out_vax("France"), index=False)


if __name__ == "__main__":
    main()
