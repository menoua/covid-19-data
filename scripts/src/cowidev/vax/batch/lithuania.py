import json

import requests
import pandas as pd

from cowidev.vax.utils.utils import make_monotonic
from cowidev.utils import paths


class Lithuania:
    location: str = "Lithuania"
    source_url_ref: str = "https://experience.arcgis.com/experience/cab84dcfe0464c2a8050a78f817924ca/page/page_3/"
    vaccine_mapping = {
        "AstraZeneca": "Oxford/AstraZeneca",
        "Johnson & Johnson": "Johnson&Johnson",
        "Moderna": "Moderna",
        "Pfizer-BioNTech": "Pfizer/BioNTech",
    }

    source_url_coverage: str = "https://services3.arcgis.com/MF53hRPmwfLccHCj/arcgis/rest/services/covid_vaccinations_chart_new/FeatureServer/0/query"
    query_params_coverage: dict = {
        "f": "json",
        "where": "municipality_code='00' AND vaccination_state<>'01dalinai'",
        "returnGeometry": False,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "date,vaccination_state,all_cum",
        "resultOffset": 0,
        "resultRecordCount": 32000,
        "resultType": "standard",
    }

    source_url_doses: str = "https://services3.arcgis.com/MF53hRPmwfLccHCj/arcgis/rest/services/covid_vaccinations_by_drug_name_new/FeatureServer/0/query"
    query_params_doses: dict = {
        "f": "json",
        "where": "municipality_code='00'",
        "returnGeometry": False,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "date,vaccines_used_cum,vaccine_name",
        "resultOffset": 0,
        "resultRecordCount": 32000,
        "resultType": "standard",
    }

    def read(self, url, params):
        res = requests.get(url, params=params)
        if res.ok:
            data = [elem["attributes"] for elem in json.loads(res.content)["features"]]
            return pd.DataFrame.from_records(data)
        raise ValueError("Source not valid/available!")

    def pipe_parse_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        df["date"] = pd.to_datetime(df["date"], unit="ms").dt.date.astype(str)
        return df

    def pipe_clean_doses(self, df: pd.DataFrame) -> pd.DataFrame:
        self.vaccine_start_dates = (
            df[(df.vaccines_used_cum > 0) & (df.vaccine_name != "visos")]
            .replace(self.vaccine_mapping)
            .groupby("vaccine_name", as_index=False)
            .min()
            .drop(columns="vaccines_used_cum")
        )
        return (
            df[(df.vaccines_used_cum > 0) & (df.vaccine_name == "visos")]
            .drop(columns="vaccine_name")
            .rename(columns={"vaccines_used_cum": "total_vaccinations"})
        )

    def pipe_clean_coverage(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.pivot(index="date", columns="vaccination_state", values="all_cum")
            .reset_index()
            .rename(
                columns={
                    "00visos": "people_vaccinated",
                    "02pilnai": "people_fully_vaccinated",
                    "03pakartotinai": "total_boosters",
                }
            )
        )
        # 02pilnai actually includes only people fully vaccinated *without* boosters
        # People who get boosters are transferred from 02pilnai to 03pakartotinai
        df["people_fully_vaccinated"] = df.people_fully_vaccinated + df.total_boosters
        return df[df.people_vaccinated > 0]

    def _find_vaccines(self, date):
        vaccines = self.vaccine_start_dates.loc[self.vaccine_start_dates.date <= date, "vaccine_name"].values
        return ", ".join(sorted(vaccines))

    def pipe_add_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        df["vaccine"] = df.date.apply(self._find_vaccines)
        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url_ref,
        )

    def export(self):
        coverage = (
            self.read(self.source_url_coverage, self.query_params_coverage)
            .pipe(self.pipe_parse_dates)
            .pipe(self.pipe_clean_coverage)
        )
        doses = (
            self.read(self.source_url_doses, self.query_params_doses)
            .pipe(self.pipe_parse_dates)
            .pipe(self.pipe_clean_doses)
        )
        df = (
            pd.merge(coverage, doses, how="inner", on="date")
            .pipe(self.pipe_add_vaccines)
            .pipe(self.pipe_metadata)
            .pipe(make_monotonic, max_removed_rows=20)
        )
        df.to_csv(paths.out_vax(self.location), index=False)


def main():
    Lithuania().export()
