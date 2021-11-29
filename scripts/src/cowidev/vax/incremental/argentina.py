import requests

import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.utils.clean.dates import localdate


class Argentina:
    location = "Argentina"
    source_url_ref = "https://www.argentina.gob.ar/coronavirus/vacuna/aplicadas"
    source_url = "https://coronavirus.msal.gov.ar/vacunas/d/8wdHBOsMk/seguimiento-vacunacion-covid/api/datasources/proxy/1/query"

    def read(self) -> pd.DataFrame:

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:94.0) Gecko/20100101 Firefox/94.0",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://coronavirus.msal.gov.ar/vacunas/d/8wdHBOsMk/seguimiento-vacunacion-covid/d/8wdHBOsMk/seguimiento-vacunacion-covid?orgId=1&refresh=15m%3F",
            "content-type": "application/json",
            "x-grafana-org-id": "1",
            "Origin": "https://coronavirus.msal.gov.ar",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        query = (
            '{"app":"dashboard","requestId":"Q101","timezone":"","panelId":4,"dashboardId":3,"range":{"raw":{"from":"2020-12-29T03:00:00.000Z","to":"now"}},"timeInfo":"","interval":"6h","intervalMs":21600000,"targets":[{"data":null,"target":"distribucion_aplicacion_utilidad_provincia_tabla_publico","refId":"A","hide":false,"type":"table"}],"maxDataPoints":9999,"scopedVars":{"__from":{"text":"1609210800000","value":"1609210800000"},"__dashboard":{"value":{"name":"Seguimiento'
            " vacunaci\xf3n"
            ' Covid","uid":"8wdHBOsMk"}},"__org":{"value":{"name":"minsal","id":0}},"__interval":{"text":"6h","value":"6h"},"__interval_ms":{"text":"21600000","value":21600000}},"startTime":1637056461969,"rangeRaw":{"from":"2020-12-29T03:00:00.000Z","to":"now"},"adhocFilters":[]}'
        )

        json_data = requests.post(self.source_url, headers=headers, data=query).json()
        for row in json_data[0]["rows"]:
            if row[0] == "Totales":
                data = row
                break

        data = pd.Series(
            {
                "people_vaccinated": data[2],
                "people_fully_vaccinated": data[3],
                "total_vaccinations": data[7],
                "total_boosters": data[5] + data[6],
            }
        )
        assert data.total_vaccinations >= data.people_vaccinated >= data.people_fully_vaccinated
        assert data.total_vaccinations >= data.total_boosters

        return data

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "date", localdate("America/Argentina/Buenos_Aires"))

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url_ref)

    def pipe_vaccines(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds, "vaccine", "CanSino, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V"
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_date).pipe(self.pipe_location).pipe(self.pipe_source).pipe(self.pipe_vaccines)

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
    Argentina().export()
