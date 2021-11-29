from datetime import datetime, timedelta

import pandas as pd

from cowidev.utils.clean import clean_date
from cowidev.utils.web import request_json
from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.vax.utils.files import load_query


class Poland:
    location: str = "Poland"
    source_url: str = (
        "https://services-eu1.arcgis.com/zk7YlClTgerl62BY/ArcGIS/rest/services/widok_global_szczepienia_actual/"
        "FeatureServer/0/query"
    )
    source_url_ref: str = "https://www.gov.pl/web/szczepimysie/raport-szczepien-przeciwko-covid-19"
    columns_rename: dict = {
        "SZCZEPIENIA_SUMA": "total_vaccinations",
        "DAWKA_1_SUMA": "people_vaccinated",
        "zaszczepieni_finalnie": "people_fully_vaccinated",
        "dawka_3_suma": "dose_3",
        "dawka_przypominajaca": "total_boosters",
        "Data": "date",
    }

    def read(self) -> pd.Series:
        params = load_query("poland-all", to_str=False)
        data = request_json(self.source_url, params=params)["features"][0]["attributes"]
        return pd.Series(data)

    def pipe_rename_columns(self, ds: pd.Series) -> pd.Series:
        return ds.rename(self.columns_rename)

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        ds.loc["date"] = clean_date(datetime.fromtimestamp(ds.date / 1000) - timedelta(days=1))
        return ds

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url_ref)

    def pipe_boosters(self, ds: pd.Series) -> pd.Series:
        ds["total_boosters"] = ds[["dose_3", "total_boosters"]].sum()
        return ds.drop(index=["dose_3"])

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_date)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_source)
            .pipe(self.pipe_boosters)
        )

    def export(self):
        """Generalized."""
        data = self.read().pipe(self.pipeline)
        print(data["total_boosters"])
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            total_boosters=data["total_boosters"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    Poland().export()


if __name__ == "__main__":
    main()
