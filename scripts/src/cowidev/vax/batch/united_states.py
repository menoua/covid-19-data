import os
from glob import glob
import requests

import pandas as pd

from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.utils import paths


class UnitedStates:
    def __init__(self):
        self.source_url = (
            "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_jurisdictional_trends_by_US"
        )
        self.source_url_age = "https://data.cdc.gov/resource/km4m-vcsb.json"
        self.location = "United States"

    ### Main processing ###

    def read(self) -> pd.DataFrame:
        data = requests.get(
            "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_jurisdictional_trends_by_US"
        ).json()
        return pd.DataFrame.from_records(data["vaccination_jurisdictional_trends_Geography"])

    def pipe_clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df[
                [
                    "Date",
                    "LongName",
                    "Administered_Cumulative",
                    "Admin_Dose_1_Cumulative",
                    "Series_Complete_Cumulative",
                    "Cumulative_Count_Booster",
                ]
            ]
            .rename(
                columns={
                    "Date": "date",
                    "LongName": "location",
                    "Administered_Cumulative": "total_vaccinations",
                    "Admin_Dose_1_Cumulative": "people_vaccinated",
                    "Series_Complete_Cumulative": "people_fully_vaccinated",
                    "Cumulative_Count_Booster": "total_boosters",
                }
            )
            .dropna(subset=["total_vaccinations"])
            .sort_values("date")
            .drop_duplicates(subset=["total_vaccinations"], keep="first")
        )
        return df

    def pipe_add_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url="https://covid.cdc.gov/covid-data-tracker/#vaccination-trends")

    def pipe_add_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccines = pd.DataFrame.from_dict(
            {
                0: ["Pfizer/BioNTech", "2020-12-01"],
                1: ["Moderna, Pfizer/BioNTech", "2020-12-23"],
                2: ["Johnson&Johnson, Moderna, Pfizer/BioNTech", "2021-03-05"],
            },
            orient="index",
            columns=["vaccine", "date"],
        )
        df = df.merge(vaccines, on="date", how="outer").sort_values("date")
        df["vaccine"] = df.vaccine.ffill()
        return df.dropna(subset=["source_url"])

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_clean_data).pipe(self.pipe_add_source).pipe(self.pipe_add_vaccines)

    ### Manufacturer processing ###

    def read_manufacturer(self) -> pd.DataFrame:
        vaccine_cols = [
            "Administered_Pfizer",
            "Administered_Moderna",
            "Administered_Janssen",
        ]
        dfs = []
        for file in glob(os.path.join(paths.SCRIPTS.INPUT_CDC_VAX, "cdc_data_*.csv")):
            try:
                df = pd.read_csv(file)
                for vc in vaccine_cols:
                    if vc not in df.columns:
                        df[vc] = pd.NA
                df = df[["Date", "LongName"] + vaccine_cols]
                dfs.append(df)
            except Exception:
                continue
        df = pd.concat(dfs)
        df = (
            df[df.LongName == "United States"]
            .sort_values("Date")
            .rename(
                columns={
                    "Date": "date",
                    "LongName": "location",
                    "Administered_Pfizer": "Pfizer/BioNTech",
                    "Administered_Moderna": "Moderna",
                    "Administered_Janssen": "Johnson&Johnson",
                }
            )
        )
        df = df.melt(["date", "location"], var_name="vaccine", value_name="total_vaccinations")
        df = df.dropna(subset=["total_vaccinations"])
        return df

    def export(self):
        self.read().pipe(self.pipeline).to_csv(paths.out_vax(self.location), index=False)

        df_manufacturer = self.read_manufacturer()
        df_manufacturer.to_csv(paths.out_vax(self.location, manufacturer=True), index=False)
        export_metadata_manufacturer(
            df_manufacturer,
            "Centers for Disease Control and Prevention",
            "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data",
        )


def main():
    UnitedStates().export()


if __name__ == "__main__":
    main()
