import pandas as pd

from cowidev.utils.web import request_json
from cowidev.utils import paths


def main():

    url = "https://services6.arcgis.com/bKYAIlQgwHslVRaK/arcgis/rest/services/Vaccination_Individual_Total/FeatureServer/0/query?f=json&cacheHint=true&outFields=*&resultType=standard&returnGeometry=false&spatialRel=esriSpatialRelIntersects&where=1%3D1"

    data = request_json(url)

    df = pd.DataFrame.from_records(elem["attributes"] for elem in data["features"])

    df = df.drop(columns=["ObjectId", "LastValue", "Total_Individuals"])

    df = df.rename(
        columns={
            "Reportdt": "date",
            "Total_Vaccinations": "total_vaccinations",
            "FirstDose": "people_vaccinated",
            "SecondDose": "people_fully_vaccinated",
        }
    )

    df["date"] = pd.to_datetime(df.date, unit="ms").dt.date.astype(str)

    df = df.groupby("date", as_index=False).max()

    df.loc[:, "location"] = "Saudi Arabia"
    df.loc[:, "vaccine"] = "Pfizer/BioNTech"
    df.loc[df.date >= "2021-02-18", "vaccine"] = "Oxford/AstraZeneca, Pfizer/BioNTech"
    df.loc[:, "source_url"] = "https://covid19.moh.gov.sa/"

    df = df[df.total_vaccinations > 0].sort_values("date")

    # The data contains an error that creates a negative change
    df = df[df.date != "2021-03-03"]

    df.to_csv(paths.out_vax("Saudi Arabia"), index=False)


if __name__ == "__main__":
    main()
