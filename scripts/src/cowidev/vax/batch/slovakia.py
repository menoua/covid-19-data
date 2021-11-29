import pandas as pd
from cowidev.utils import paths


def import_iza():

    iza = pd.read_csv(
        (
            "https://github.com/Institut-Zdravotnych-Analyz/covid19-data/raw/main/Vaccination/"
            "OpenData_Slovakia_Vaccination_Regions.csv"
        ),
        usecols=["Date", "first_dose", "second_dose"],
        sep=";",
    )

    iza["first_dose"] = pd.to_numeric(iza.first_dose, errors="coerce")
    iza["second_dose"] = pd.to_numeric(iza.second_dose, errors="coerce")

    iza = (
        iza.groupby("Date", as_index=False)
        .sum()
        .rename(
            columns={
                "Date": "date",
                "first_dose": "people_vaccinated",
                "second_dose": "people_fully_vaccinated",
            }
        )
        .sort_values("date")
    )

    iza["people_vaccinated"] = iza["people_vaccinated"].cumsum()
    iza["people_fully_vaccinated"] = iza["people_fully_vaccinated"].cumsum()
    iza["total_vaccinations"] = (
        iza["people_vaccinated"] + iza["people_fully_vaccinated"]
    )
    iza["people_fully_vaccinated"] = iza["people_fully_vaccinated"].replace(0, pd.NA)
    iza["source_url"] = "https://github.com/Institut-Zdravotnych-Analyz/covid19-data"

    return iza


def main():

    df = import_iza()

    df.loc[:, "location"] = "Slovakia"

    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df.date >= "2021-01-27", "vaccine"] = "Moderna, Pfizer/BioNTech"
    df.loc[
        df.date >= "2021-02-13", "vaccine"
    ] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sputnik V"

    df.to_csv(paths.out_vax("Slovakia"), index=False)


if __name__ == "__main__":
    main()
