import re
import time

import pandas as pd

from cowidev.utils.clean import clean_count, extract_clean_date
from cowidev.utils.web.scraping import get_driver
from cowidev.vax.utils.incremental import merge_with_current_data
from cowidev.utils import paths


class China:
    location: str = "China"
    source_url: str = "http://www.nhc.gov.cn/xcs/yqjzqk/list_gzbd.shtml"
    regex: dict = {
        "date": r"截至(20\d{2})年(\d{1,2})月(\d{1,2})日",
        "total_vaccinations": "([\d\.]+万)剂次。",
    }

    def read(self, last_update: str):
        data = []
        with get_driver(firefox=True) as driver:
            driver.get(self.source_url)
            time.sleep(5)
            links = self._get_links(driver)
            for link in links:
                data_ = self._parse_data(driver, link)
                if data_["date"] <= last_update:
                    # print(data_["date"], "<", last_update)
                    break
                data.append(data_)
        return pd.DataFrame(data)

    def _parse_data(self, driver, url):
        driver.get(url)
        elem = driver.find_element_by_id("xw_box")
        return {
            "date": extract_clean_date(elem.text, self.regex["date"], "%Y %m %d"),
            "total_vaccinations": clean_count(re.search(self.regex["total_vaccinations"], elem.text).group(1)),
            "source_url": url,
        }

    def _get_links(self, driver) -> list:
        elems = driver.find_elements_by_css_selector("li>a")
        return [elem.get_property("href") for elem in elems]

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
        )

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="CanSino, Sinopharm/Beijing, Sinopharm/Wuhan, Sinovac, ZF2001")

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_metadata).pipe(self.pipe_vaccine)

    def export(self):
        output_file = paths.out_vax(self.location)
        last_update = pd.read_csv(output_file).date.max()
        df = self.read(last_update)
        if not df.empty:
            df = df.pipe(self.pipeline)
            # print(df.tail())
            df = merge_with_current_data(df, output_file)
            df.to_csv(output_file, index=False)


def main():
    China().export()
