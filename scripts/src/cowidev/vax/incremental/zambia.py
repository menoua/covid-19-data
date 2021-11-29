from datetime import datetime

import pandas as pd

from cowidev.utils.clean import clean_date
from cowidev.utils.web import request_json
from cowidev.vax.utils.incremental import enrich_data, increment


class Zambia:
    def __init__(self) -> None:
        self.location = "Zambia"
        self.source_url = (
            "https://services7.arcgis.com/OwPYxdqWv7612O7N/arcgis/rest/services/"
            "service_ef4ce56ba48a44ef82991dcf85f62f71/FeatureServer/0/query?f=json&&cacheHint=true&resultOffset=0&"
            "resultRecordCount=100&where=1=1&outFields=*&resultType=standard&returnGeometry=false&"
            "spatialRel=esriSpatialRelIntersects"
        )
        self.source_url_ref = "https://rtc-planning.maps.arcgis.com/apps/dashboards/3b3a01c1d8444932ba075fb44b119b63"

    def read(self):
        data = request_json(self.source_url)["features"][0]["attributes"]
        return pd.Series(
            {
                "total_vaccinations": data["Vaccine_total"],
                "people_fully_vaccinated": data["Vaccine_total_last24"],
                "date": clean_date(datetime.fromtimestamp(data["Date"] / 1000)),
            }
        )

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url_ref)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Johnson&Johnson, Oxford/AstraZeneca, Sinopharm/Beijing")

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_source).pipe(self.pipe_vaccine)

    def export(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    Zambia().export()
