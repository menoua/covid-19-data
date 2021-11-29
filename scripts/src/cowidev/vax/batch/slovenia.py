import pandas as pd
from cowidev.utils import paths


def main():

    df = (
        pd.read_csv(
            "https://raw.githubusercontent.com/sledilnik/data/master/csv/vaccination.csv",
            usecols=[
                "date",
                "vaccination.administered.todate",
                "vaccination.administered2nd.todate",
                "vaccination.administered3rd.todate",
            ],
        )
        .rename(
            columns={
                "vaccination.administered.todate": "people_vaccinated",
                "vaccination.administered2nd.todate": "people_fully_vaccinated",
                "vaccination.administered3rd.todate": "total_boosters",
            }
        )
        .dropna(subset=["people_vaccinated"])
    )

    df["total_vaccinations"] = (
        df["people_vaccinated"] + df["people_fully_vaccinated"].fillna(0) + df["total_boosters"].fillna(0)
    )

    df["location"] = "Slovenia"
    df["source_url"] = "https://covid-19.sledilnik.org/en/stats"
    df["vaccine"] = "Oxford/AstraZeneca, Pfizer/BioNTech"

    df.to_csv(paths.out_vax("Slovenia"), index=False)


if __name__ == "__main__":
    main()
