import datetime

import pandas as pd
import numpy as np
import requests

from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.utils import paths


class Ukraine:
    # it is expected to use Novavax vaccine as well in future,
    # if so, this script should be updated

    def __init__(self):
        self.source_url = "https://health-security.rnbo.gov.ua"
        self.source_api_url = "https://health-security.rnbo.gov.ua/api/vaccination/process/chart"
        self.location = "Ukraine"

    def _load_dose_data(self, dose_param, colname):
        doses_api = requests.get(f"{self.source_api_url}?dose={dose_param}").json()

        data_dict = {
            "date": doses_api["daily"]["dates"],
            f"{colname}_moderna": doses_api["daily"]["cumulative"]["Moderna"],
            f"{colname}_astrazeneca": doses_api["daily"]["cumulative"]["AstraZeneca"],
            f"{colname}_phizer": doses_api["daily"]["cumulative"]["Pfizer-BioNTech"],
            f"{colname}_sinovac": doses_api["daily"]["cumulative"]["Sinovac (CoronaVac)"],
        }

        if dose_param == "1":
            data_dict[f"{colname}_jnj"] = doses_api["daily"]["cumulative"]["Johnson & Johnson"]

        df = pd.DataFrame(
            {
                "date": [datetime.datetime.strptime(x, "%Y-%m-%d") for x in doses_api["daily"]["dates"]],
                f"{colname}_moderna": doses_api["daily"]["cumulative"]["Moderna"],
                f"{colname}_astrazeneca": doses_api["daily"]["cumulative"]["AstraZeneca"],
                f"{colname}_phizer": doses_api["daily"]["cumulative"]["Pfizer-BioNTech"],
                f"{colname}_jnj": doses_api["daily"]["cumulative"].get("Johnson & Johnson", 0),
                f"{colname}_sinovac": doses_api["daily"]["cumulative"]["Sinovac (CoronaVac)"],
            },
        )

        if dose_param == "2":
            df[f"{colname}_jnj"] = 0

        df[f"{colname}_total"] = (
            df[f"{colname}_moderna"]
            + df[f"{colname}_astrazeneca"]
            + df[f"{colname}_phizer"]
            + df[f"{colname}_jnj"]
            + df[f"{colname}_sinovac"]
        )

        return df

    def read(self):
        first_dose_df = self._load_dose_data(dose_param="1", colname="first_dose")
        second_dose_df = self._load_dose_data(dose_param="2", colname="second_dose")
        total_df = first_dose_df.join(second_dose_df.set_index("date"), on=["date"])

        for col in ["total", "moderna", "astrazeneca", "phizer", "jnj", "sinovac"]:
            total_df[f"all_doses_{col}"] = total_df[f"first_dose_{col}"] + total_df[f"second_dose_{col}"]

        total_df["location"] = self.location
        total_df["source_url"] = self.source_url
        return total_df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        conditions = [
            (df["date"] <= np.datetime64("2021-04-13")),
            (df["date"] > np.datetime64("2021-04-13")) & (df["date"] <= np.datetime64("2021-04-17")),
            (df["date"] > np.datetime64("2021-04-17")) & (df["date"] <= np.datetime64("2021-05-21")),
            (df["date"] > np.datetime64("2021-05-21")) & (df["date"] <= np.datetime64("2021-07-22")),
            (df["date"] > np.datetime64("2021-07-22")),
        ]
        values = [
            "Oxford/AstraZeneca",
            "Oxford/AstraZeneca, Sinovac",
            "Oxford/AstraZeneca, Sinovac, Pfizer/BioNTech",
            "Johnson&Johnson, Oxford/AstraZeneca, Sinovac, Pfizer/BioNTech",
            "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Sinovac, Pfizer/BioNTech",
        ]

        df["vaccine"] = np.select(conditions, values)
        df["total_vaccinations"] = df["second_dose_total"] + df["first_dose_total"]
        df["people_fully_vaccinated"] = df["second_dose_total"] + df["first_dose_jnj"]
        df.rename(columns={"first_dose_total": "people_vaccinated"}, inplace=True)
        df["date"] = df["date"].apply(lambda x: x.strftime("%Y-%m-%d"))

        return df[
            [
                "location",
                "date",
                "vaccine",
                "source_url",
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
            ]
        ].sort_values(["date"])

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccine_name = ["Oxford/AstraZeneca", "Sinovac", "Pfizer/BioNTech", "Johnson&Johnson", "Moderna"]
        column_name = [
            "all_doses_astrazeneca",
            "all_doses_sinovac",
            "all_doses_phizer",
            "all_doses_jnj",
            "all_doses_moderna",
        ]

        vac_dfs = []
        for vaccine, col in zip(vaccine_name, column_name):
            vac_df = df[["location", "date", col]]
            vac_df["vaccine"] = vaccine
            vac_df.rename(columns={col: "total_vaccinations"}, inplace=True)
            vac_df = vac_df[vac_df["total_vaccinations"] > 0]

            vac_dfs.append(vac_df[["location", "date", "vaccine", "total_vaccinations"]])

        return pd.concat(vac_dfs, ignore_index=True).sort_values(["date", "vaccine"])

    def export(self):
        # Load data
        df = self.read()
        # Export main
        df.pipe(self.pipeline).to_csv(paths.out_vax(self.location), index=False)
        # Export manufacturer data
        df_man = df.pipe(self.pipeline_manufacturer)
        df_man.to_csv(paths.out_vax(self.location, manufacturer=True), index=False)
        export_metadata_manufacturer(
            df_man,
            "National Security and Defense Council of Ukraine",
            self.source_url,
        )


def main():
    Ukraine().export()


if __name__ == "__main__":
    main()
