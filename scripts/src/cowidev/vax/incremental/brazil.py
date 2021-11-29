import time

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_driver, set_download_settings
from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.vax.utils.utils import get_latest_file


class Brazil:
    def __init__(self) -> None:
        self.location = "Brazil"
        self.source_url = "https://infoms.saude.gov.br/extensions/DEMAS_C19_Vacina_v2/DEMAS_C19_Vacina_v2.html"
        self._download_path = "/tmp"

    def read(self) -> pd.Series:
        return self._parse_data()

    def _parse_data(self) -> pd.Series:
        with get_driver() as driver:
            set_download_settings(driver, self._download_path)
            driver.get(self.source_url)
            data_blocks = WebDriverWait(driver, 20).until(
                EC.visibility_of_all_elements_located((By.CLASS_NAME, "sn-kpi-data"))
            )
            for block in data_blocks:
                block_title = block.find_element_by_class_name("sn-kpi-measure-title").get_attribute("title")
                if "Total de Doses Aplicadas (Dose1)" in block_title:
                    first_doses = clean_count(block.find_element_by_class_name("sn-kpi-value").text)
                elif "Total de Doses Aplicadas (Doses 2 e Única)" in block_title:
                    people_fully_vaccinated = clean_count(block.find_element_by_class_name("sn-kpi-value").text)
                elif "Total de Doses Aplicadas" in block_title:
                    total_vaccinations = clean_count(block.find_element_by_class_name("sn-kpi-value").text)
                elif "Dose Adicional" in block_title:
                    additional_doses = clean_count(block.find_element_by_class_name("sn-kpi-value").text)
                elif "Dose Reforço" in block_title:
                    booster_doses = clean_count(block.find_element_by_class_name("sn-kpi-value").text)

            # unique_doses = self._parse_unique_doses(driver)
            # All download buttons on the dashboard have stopped working as of November 1, 2021
            # Unique doses are now estimated as a fixed rate of all administered doses
            # This ratio is updated weekly, based on the chart "Doses Aplicadas por Laboratorio"
            unique_doses_ratio = 0.017
            unique_doses = round(total_vaccinations * unique_doses_ratio)

        ds = pd.Series(
            {
                "total_vaccinations": total_vaccinations,
                "people_vaccinated": first_doses + unique_doses,
                "people_fully_vaccinated": people_fully_vaccinated,
                "total_boosters": additional_doses + booster_doses,
            }
        )
        return ds

    def _parse_unique_doses(self, driver):
        driver.find_element_by_class_name("sn-kpi").click()
        for _ in range(30):
            driver.find_element_by_tag_name("html").send_keys(Keys.DOWN)
        driver.find_element_by_id("QV1-G13B-menu").click()
        time.sleep(5)
        path = get_latest_file(self._download_path, "xlsx")
        df = pd.read_excel(path)
        unique_doses = df.loc[df.Fabricante == "JANSSEN", "Doses Aplicadas"].sum().item()
        return unique_doses

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        date = localdate("Brazil/East")
        return enrich_data(ds, "date", date)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Johnson&Johnson, Pfizer/BioNTech, Oxford/AstraZeneca, Sinovac",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_date).pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

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
    Brazil().export()
