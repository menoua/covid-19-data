from collections import defaultdict
import copy

import pandas as pd

from cowidev.utils.web import request_json
from cowidev.vax.utils.orgs import SPC_COUNTRIES
from cowidev.vax.utils.files import load_data
from cowidev.vax.utils.utils import make_monotonic
from cowidev.utils import paths


metrics_mapping = {
    "COVIDVACAD1": "people_vaccinated",
    "COVIDVACAD2": "people_fully_vaccinated",
    "COVIDVACADT": "total_vaccinations",
}

# Dictionary containing vaccines being used in each country and their start date. Element 'default' is used for all
# countries not explicitly defined. None defaults to first date of vaccination campaign.
vaccines_startdates = {
    "New Caledonia": [
        ["Pfizer/BioNTech", None],
    ],
    "French Polynesia": [
        ["Johnson&Johnson, Pfizer/BioNTech", None],
    ],
    "Tokelau": [
        ["Pfizer/BioNTech", None],
    ],
    "Cook Islands": [
        ["Pfizer/BioNTech", None],
    ],
    "Wallis and Futuna": [
        ["Moderna", None],
    ],
    "default": [
        ["Oxford/AstraZeneca", None],
    ],
}


class SPC:
    def __init__(self, source_url: str):
        self.source_url = source_url

    def read(self):
        # Get data
        data = request_json(self.source_url)
        return self.parse_data(data)

    def parse_data(self, data: dict):
        series = data["data"]["dataSets"][0]["series"]
        country_info = self._parse_country_info(data)
        metrics_info = self._parse_metrics_info(data)
        date_info = self._parse_date_info(data)
        vaccination_data = defaultdict(dict)
        for k, v in series.items():
            _, country_idx, metric_idx = k.split(":")
            if metric_idx in metrics_info:
                vaccination_data[country_info[country_idx]][metrics_info[metric_idx]] = self._build_data_array(
                    v["observations"], date_info
                )
        return self._build_df_list(vaccination_data)

    def _parse_country_info(self, data: dict):
        # Get country info
        country_info = data["data"]["structure"]["dimensions"]["series"][1]
        if country_info["id"] != "GEO_PICT":
            raise AttributeError("JSON data has changed")
        return {str(i): SPC_COUNTRIES[c["id"]] for i, c in enumerate(country_info["values"])}

    def _parse_metrics_info(self, data: dict):
        # Get metrics info
        metrics_info = data["data"]["structure"]["dimensions"]["series"][2]
        if metrics_info["id"] != "INDICATOR":
            raise AttributeError("JSON data has changed")
        return {
            str(i): metrics_mapping[m["id"]]
            for i, m in enumerate(metrics_info["values"])
            if m["id"] in metrics_mapping
        }

    def _parse_date_info(self, data: dict):
        # Get date info
        date_info = data["data"]["structure"]["dimensions"]["observation"][0]["values"]
        return {str(i): d["name"] for i, d in enumerate(date_info)}

    def _build_data_array(self, observations: dict, date_info: dict):
        return {date_info[k]: v[0] if len(v) == 1 else None for k, v in observations.items()}

    def _build_df_list(self, data: dict):
        for k, v in data.items():
            data[k] = self._build_df(v, k)
        return data

    def _build_df(self, dix: dict, country: str):
        df = (
            pd.DataFrame(dix)
            .dropna(how="all")
            .replace("", None)
            .astype("Int64")
            .drop_duplicates(keep="first")
            .reset_index()
            .rename(columns={"index": "date"})
            .sort_values(by="date")
            .assign(
                location=country,
                source_url=(
                    "https://stats.pacificdata.org/vis?tm=covid&pg=0&df[ds]=SPC2&df[id]=DF_COVID_VACCINATION&df[ag]=SPC&df"
                    "[vs]=1.0"
                ),
            )
        )
        # Merge with legacy (spreadsheet)
        if country in ["Fiji", "Nauru", "Vanuatu"]:
            df = df.pipe(self.pipe_merge_legacy, country)
        # Drop duplicates
        df = df.pipe(self.pipe_drop_duplicates)
        # Enforce data consistency
        df.loc[
            df.people_vaccinated < df.people_fully_vaccinated,
            ["people_vaccinated", "people_fully_vaccinated"],
        ] = pd.NA
        # Make monotonic
        df = df.pipe(make_monotonic)
        # Add vaccine info
        df = df.pipe(self.pipe_vacine, country)
        return df

    def pipe_merge_legacy(self, df: pd.DataFrame, country: str) -> pd.DataFrame:
        country = country.lower().replace(" ", "-")
        df_legacy = load_data(f"{country}-legacy")
        df_legacy = df_legacy[~df_legacy.date.isin(df.date)]
        return pd.concat([df, df_legacy]).sort_values("date")

    def pipe_drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        column_metrics = [
            "people_vaccinated",
            "total_vaccinations",
            "people_fully_vaccinated",
        ]
        msk = df.people_vaccinated == 0
        df.loc[msk, "people_fully_vaccinated"] = pd.NA
        df = df.drop_duplicates(subset=column_metrics)
        return df

    def pipe_vacine(self, df: pd.DataFrame, country: str) -> pd.DataFrame:
        # print(country)
        date_min = df.date.min()
        vax_date_mapping = self._pretty_vaxdates(country, date_min)
        # print(vax_date_mapping)
        def _enrich_vaccine(date: str) -> str:
            for dt, vaccines in vax_date_mapping:
                # print(f"{date}, {dt}: {vaccines}")
                if date >= dt:
                    return vaccines
            raise ValueError(f"Invalid date {date} in DataFrame!")

        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def _pretty_vaxdates(self, country, date_min):
        if country not in vaccines_startdates:
            country = "default"
        records = copy.deepcopy(vaccines_startdates[country])
        # Substitute None by minimum date
        for i, (_, dt) in enumerate(records):
            if dt is None:
                records[i][1] = date_min
        # print(records)
        records = sorted(records, key=lambda x: x[1])
        # Build mapping dictionary
        vax_date_mapping = [
            (dt, ", ".join(sorted(r[0] for r in records[: i + 1]))) for i, (vax, dt) in enumerate(records)
        ]
        return vax_date_mapping

    def to_csv(self):
        data = self.read()
        for country, df in data.items():
            df.to_csv(paths.out_vax(country), index=False)


def main():
    country_codes_url = "+".join(SPC_COUNTRIES.keys())
    SPC(
        source_url=(
            f"https://stats-nsi-stable.pacificdata.org/rest/data/SPC,DF_COVID_VACCINATION,1.0/D.{country_codes_url}.?"
            "startPeriod=2021-02-02&format=jsondata"
        ),
    ).to_csv()


if __name__ == "__main__":
    main()
