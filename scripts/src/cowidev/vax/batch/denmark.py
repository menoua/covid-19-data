import zipfile
import io
import os
import tempfile
from datetime import datetime

import requests
import pandas as pd

from cowidev.utils.clean import clean_date_series
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.checks import VACCINES_ONE_DOSE
from cowidev.utils import paths


SEPARATOR = ";"
SEPARATOR_ALT = ","


class Denmark:
    def __init__(self):
        self.location = "Denmark"
        # self.source_url_ref = "https://covid19.ssi.dk/overvagningsdata/vaccinationstilslutning"
        self.source_url_ref = "https://covid19.ssi.dk/overvagningsdata/download-fil-med-vaccinationsdata"
        self.date_limit_one_dose = "2021-05-27"
        self.vaccines_mapping = {
            "AstraZeneca Covid-19 vaccine": "Oxford/AstraZeneca",
            "Janssen COVID-19 vaccine": "Johnson&Johnson",
            "Moderna Covid-19 Vaccine": "Moderna",
            "Moderna/Spikevax Covid-19 Vacc.": "Moderna",
            "Moderna/Spikevax Covid-19 0,5 ml": "Moderna",
            "Pfizer BioNTech Covid-19 vacc": "Pfizer/BioNTech",
        }
        self.regions_accepted = {
            "Nordjylland",
            "Midtjylland",
            "Syddanmark",
            "Hovedstaden",
            "Sjælland",
        }

    @property
    def num_days_since_launch_single_dose(self):
        return (datetime.now() - datetime.strptime(self.date_limit_one_dose, "%Y-%m-%d")).days

    def read(self) -> str:
        url = self._parse_link_zip()
        with tempfile.TemporaryDirectory() as tf:
            # Download and extract
            self._download_data(url, tf)
            df = self._parse_data(tf, load_boosters=True)
            total_vaccinations_latest = self._parse_total_vaccinations(tf)
            df.loc[df["Vaccinedato"] == df["Vaccinedato"].max(), "total_vaccinations"] = total_vaccinations_latest
        return df

    def _parse_link_zip(self):
        soup = get_soup(self.source_url_ref)
        url = soup.find("a", string="Download her").get("href")
        return url

    def _download_data(self, url, output_path):
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(output_path)

    def _parse_data(self, path, load_boosters: bool = False):
        df_dose1 = self._load_df_metric(path, "PaabegVacc_daek_DK_prdag.csv", "Kumuleret antal påbegyndt vacc.")
        df_fully = self._load_df_metric(path, "FaerdigVacc_daekning_DK_prdag.csv", "Kumuleret antal færdigvacc.")
        df = df_fully.merge(df_dose1, on="Vaccinedato", how="outer")
        if load_boosters:
            df_boosters = self._load_boosters(path, "Revacc1_region_dag.csv")
            df = pd.merge(df, df_boosters, on="Vaccinedato", how="outer")
        return df.sort_values("Vaccinedato")

    def _load_boosters(self, path, filename: str) -> pd.DataFrame:
        df = (
            pd.read_csv(
                os.path.join(path, "Vaccine_DB", filename),
                encoding="iso-8859-1",
                usecols=["Revacc. 1 dato", "Antal revacc. 1"],
                sep=";",
            )
            .rename(columns={"Revacc. 1 dato": "Vaccinedato"})
            .groupby("Vaccinedato", as_index=False)
            .sum()
            .sort_values("Vaccinedato")
        )
        df["Antal revacc. 1"] = df["Antal revacc. 1"].cumsum()
        return df

    def _load_df_metric(self, path, filename: str, metric_name: str):
        try:
            df = pd.read_csv(
                os.path.join(path, "Vaccine_DB", filename),
                encoding="iso-8859-1",
                usecols=["Vaccinedato", "geo", metric_name],
                sep=SEPARATOR,
            )
        except ValueError:
            df = pd.read_csv(
                os.path.join(path, "Vaccine_DB", filename),
                encoding="iso-8859-1",
                usecols=["Vaccinedato", "geo", metric_name],
                sep=SEPARATOR_ALT,
            )
        return df[df.geo == "Nationalt"].drop(columns=["geo"])

    def _parse_total_vaccinations(self, path):
        df = pd.read_csv(
            os.path.join(path, "Vaccine_DB", "Vaccinationstyper_regioner.csv"),
            encoding="iso-8859-1",
            sep=SEPARATOR,
        )
        if len(df.columns) == 1:
            df = pd.read_csv(
                os.path.join(path, "Vaccine_DB", "Vaccinationstyper_regioner.csv"),
                encoding="iso-8859-1",
                sep=SEPARATOR_ALT,
            )
        # Check 1/2
        self._check_df_vax_1(df)
        # Rename columns
        df = df.assign(
            vaccine=df["Vaccinenavn"].replace(self.vaccines_mapping),
            dose_1=df["Antal første vacc."],
            dose_2=df["Antal faerdigvacc."],
        )
        # Check 2/2
        mask = df.vaccine.isin(VACCINES_ONE_DOSE)
        self._check_df_vax_2(df, mask)
        # Get value
        total_1 = df.dose_1.sum()
        total_2 = df.loc[~mask, "dose_2"].sum()
        total_vaccinations = total_1 + total_2
        return total_vaccinations

    def _check_df_vax_1(self, df):
        # print(list(df.columns))
        vaccines_wrong = set(df.Vaccinenavn).difference(self.vaccines_mapping)
        if vaccines_wrong:
            raise ValueError(f"Unknown vaccine(s) {vaccines_wrong}")
        regions_wrong = set(df.Regionsnavn).difference(self.regions_accepted)
        if vaccines_wrong:
            raise ValueError(f"Unknown region(s) {regions_wrong}")

    def _check_df_vax_2(self, df, mask):
        if (df.loc[mask, "dose_1"] - df.loc[mask, "dose_2"]).sum() != 0:
            raise ValueError(f"First and second dose counts for single-shot vaccines should be equal.")

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "Vaccinedato": "date",
                "Kumuleret antal færdigvacc.": "people_fully_vaccinated",
                "Kumuleret antal påbegyndt vacc.": "people_vaccinated",
                "Antal revacc. 1": "total_boosters",
            }
        )

    def pipe_format_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date))

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(
            people_vaccinated=df.people_vaccinated.ffill(),
            people_fully_vaccinated=df.people_fully_vaccinated.ffill(),
            total_boosters=df.total_boosters.ffill(),
        )
        mask = df.date < self.date_limit_one_dose
        df.loc[mask, "total_vaccinations"] = df.loc[mask, "people_vaccinated"] + df.loc[
            mask, "people_fully_vaccinated"
        ].fillna(0)
        # Uncomment to backfill total_vaccinations
        df = df.pipe(self.pipe_total_vax_bfill, n_days=self.num_days_since_launch_single_dose)
        # Correct total_vaccinations with boosters
        df.loc[:, "total_vaccinations"] = df["total_vaccinations"] + df["total_boosters"]
        return df

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            if date >= self.date_limit_one_dose:
                return "Johnson&Johnson, Moderna, Pfizer/BioNTech"
            if date >= "2021-04-14":
                return "Moderna, Pfizer/BioNTech"
            if date >= "2021-02-08":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            if date >= "2021-01-13":
                return "Moderna, Pfizer/BioNTech"
            return "Pfizer/BioNTech"

        return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url_ref)

    def pipe_filter_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df.date >= "2020-12-01"]
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_format_date)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_filter_rows)
        )

    def export(self):
        df = self.read()
        df.pipe(self.pipeline).to_csv(paths.out_vax(self.location), index=False)

    def pipe_total_vax_bfill(self, df: pd.DataFrame, n_days: int) -> pd.DataFrame:
        soup = get_soup(self.source_url_ref)
        links = self._get_zip_links(soup)
        links = links[:n_days]
        df = self._backfill_total_vaccinations(df, links)
        return df

    def _get_zip_links(self, soup):
        links = [x.a.get("href") for x in soup.find_all("h5")]
        return links

    def _get_total_vax(self, url):
        with tempfile.TemporaryDirectory() as tf:
            self._download_data(url, tf)
            df = self._parse_data(tf)
            total_vaccinations_latest = self._parse_total_vaccinations(tf)
        return total_vaccinations_latest, df.Vaccinedato.max()

    def _backfill_total_vaccinations(self, df: pd.DataFrame, links: list):
        for link in links:
            # print(link)
            total_vaccinations_latest, date = self._get_total_vax(link)
            df.loc[df["date"] == date, "total_vaccinations"] = total_vaccinations_latest
        return df


def main():
    Denmark().export()
