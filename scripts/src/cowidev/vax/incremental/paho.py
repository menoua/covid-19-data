import os
import time
from glob import glob

import pandas as pd

from cowidev.utils.clean import clean_date
from cowidev.utils.web.scraping import get_soup, get_driver
from cowidev.vax.utils.files import get_file_encoding
from cowidev.vax.utils.incremental import increment
from cowidev.vax.utils.orgs import WHO_VACCINES, PAHO_COUNTRIES
from cowidev.vax.cmd.utils import get_logger


logger = get_logger()


class PAHO:
    def __init__(self) -> None:
        self.source_url = "https://ais.paho.org/imm/IM_DosisAdmin-Vacunacion.asp"
        self._download_path = "/tmp"
        self.columns_mapping = {
            "Country/ Territory": "location",
            "Country code": "country_code",
            "Single dose": "single_dose",
            "First dose": "dose_1",
            "Second dose": "dose_2",
            "Complete Schedule": "people_fully_vaccinated",
            "Total doses": "total_vaccinations",
            "Additional dose": "total_boosters",
            "date": "date",
        }

    def read(self):
        url = self._parse_iframe_link()
        df = self._parse_data(url)
        return df

    def _parse_iframe_link(self):
        html = get_soup(self.source_url)
        url = html.find("iframe").get("src")
        return url

    def _parse_data(self, url: str):
        with get_driver(download_folder=self._download_path) as driver:
            # Go to page
            driver.get(url)
            time.sleep(7.5)
            # Go to tab
            driver.find_element_by_id("tableauTabbedNavigation_tab_2").click()
            time.sleep(5)
            # Download data
            self._download_csv(driver, "Crosstab", "RDT: Overview Table")
            # Load downloadded file
            filename = self._get_downloaded_filename()
            df = pd.read_csv(filename, sep="\t", encoding=get_file_encoding(filename), thousands=",")
            os.remove(filename)
            df = df.assign(date=self._parse_date(driver))
        return df

    def _download_csv(self, driver, option: str, filename: str):
        # Click on download
        driver.find_element_by_id("download-ToolbarButton").click()
        time.sleep(1)
        # Click on Crosstab
        driver.find_element_by_xpath(f"//button[contains(text(),'{option}')]").click()
        time.sleep(3)
        # Select RDT Overview option
        driver.find_element_by_xpath(f"//span[contains(text(),'{filename}')]").click()
        time.sleep(2)
        # Choose CSV
        driver.find_element_by_xpath("//div[contains(text(),'CSV')]").click()
        time.sleep(2)
        # Select RDT Overview option
        # driver.find_element_by_xpath(f"//span[contains(text(),'{filename}')]").click()
        # time.sleep(2)
        # Download
        driver.find_element_by_xpath("//button[contains(text(),'Download')]").click()
        time.sleep(5)

    def _parse_date(self, driver):
        driver.find_element_by_id("tabZoneId87").click()
        time.sleep(1)
        driver.find_element_by_id("download-ToolbarButton").click()
        time.sleep(2)
        driver.find_element_by_xpath(f"//button[contains(text(),'Data')]").click()
        time.sleep(4)
        window_after = driver.window_handles[1]
        driver.switch_to.window(window_after)
        time.sleep(2)
        date_str = driver.find_element_by_tag_name("tbody").text
        return clean_date(date_str, "%m/%d/%Y")

    def _get_downloaded_filename(self):
        files = glob(os.path.join(self._download_path, "*.csv"))
        # print(files)
        return max(files, key=os.path.getctime)

    def pipe_check_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.replace(" \[\d.*", "", regex=True)
        columns_missing = set(self.columns_mapping).difference(df.columns)
        if columns_missing:
            raise ValueError(f"Missing column fields: {columns_missing}. Present columns are: {df.columns}")
        return df

    def pipe_check_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.columns_mapping)

    def pipe_filter_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Get rows from selected countries."""
        countries_wrong = set(PAHO_COUNTRIES).difference(df.location)
        if countries_wrong:
            raise ValueError(f"Invalid country(s) {countries_wrong}")
        df = df[df.location.isin(PAHO_COUNTRIES)]
        df["location"] = df.location.replace(PAHO_COUNTRIES)
        return df

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            people_vaccinated=df["single_dose"] + df["dose_1"],
        )

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            source_url=self.source_url,
        )

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        url = "https://covid19.who.int/who-data/vaccination-data.csv"
        df_who = pd.read_csv(url, usecols=["ISO3", "VACCINES_USED"]).rename(columns={"VACCINES_USED": "vaccine"})
        df_who = df_who.dropna(subset=["vaccine"])
        df_who = df_who.assign(
            vaccine=df_who.vaccine.apply(
                lambda x: ", ".join(sorted(set(WHO_VACCINES[xx.strip()] for xx in x.split(","))))
            )
        )
        df = df.merge(df_who, left_on="country_code", right_on="ISO3")
        return df

    def pipe_select_out_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            [
                "location",
                "date",
                "vaccine",
                "source_url",
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
                "total_boosters",
            ]
        ]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_check_columns)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_filter_countries)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_select_out_cols)
        )

    def increment_countries(self, df: pd.DataFrame):
        for row in df.sort_values("location").iterrows():
            row = row[1]
            increment(
                location=row["location"],
                total_vaccinations=row["total_vaccinations"],
                people_vaccinated=row["people_vaccinated"],
                people_fully_vaccinated=row["people_fully_vaccinated"],
                total_boosters=row["total_boosters"],
                date=row["date"],
                vaccine=row["vaccine"],
                source_url=row["source_url"],
            )
            country = row["location"]
            logger.info(f"\tvax.incremental.paho.{country}: SUCCESS ✅")

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.increment_countries(df)


def main():
    PAHO().export()
