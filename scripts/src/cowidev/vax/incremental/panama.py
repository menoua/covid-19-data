import pandas as pd

from cowidev.utils.clean.dates import localdatenow
from cowidev.utils.web import request_json
from cowidev.vax.utils.incremental import increment, enrich_data


class Panama:
    location: str = "Panama"
    source_url: str = (
        "https://services8.arcgis.com/d4orHpY7OSevCg39/arcgis/rest/services/resumen_dashboard_vista/FeatureServer/0/"
        "query"
    )
    source_url_ref: str = "https://vacunas.panamasolidario.gob.pa/vacunometro"
    timezone: str = "America/Panama"

    def read(self):
        data = self._parse_data()
        return pd.Series(data=data)

    def _parse_data(self):
        response = self._api_request()
        data = response["features"][0]["attributes"]
        return {
            "total_vaccinations": data["total_vaccinations"],
            "people_vaccinated": data["dose_1"],
            "people_fully_vaccinated": data["dose_2"],
            "total_boosters": data["total_vaccinations"] - data["dose_1"] - data["dose_2"],
        }

    def _api_request(self):
        date_low = localdatenow(self.timezone)
        date_up = localdatenow(self.timezone, sum_days=1)
        params = {
            "f": "json",
            "outFields": "*",
            "outStatistics": (
                "[{'onStatisticField':'total_dosis_adminsitradas','outStatisticFieldName':'total_vaccinations','statisticType':'sum'},"
                "{'onStatisticField':'total_primera_dosis','outStatisticFieldName':'dose_1','statisticType':'sum'},"
                "{'onStatisticField':'total_segunda_dosis','outStatisticFieldName':'dose_2','statisticType':'max'}]"
            ),
            "returnGeometry": "false",
            "where": f"fecha BETWEEN timestamp '{date_low} 05:00:00' AND timestamp '{date_up} 04:59:59'",
        }
        data = request_json(self.source_url, params=params)
        return data

    def pipe_metadata(self, ds: pd.Series) -> pd.Series:
        ds = enrich_data(ds, "location", self.location)
        ds = enrich_data(ds, "source_url", self.source_url_ref)
        ds = enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech")
        ds = enrich_data(ds, "date", localdatenow(self.timezone))
        return ds

    def pipeline(self, ds: pd.Series):
        return ds.pipe(self.pipe_metadata)

    def export(self):
        data = self.read().pipe(self.pipeline)
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
    Panama().export()
