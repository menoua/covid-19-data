from cowidev.utils.web import get_driver
from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.utils.incremental import merge_with_current_data
from cowidev.utils import paths

import re
import pandas as pd


class Uzbekistan:
    location: str = "Uzbekistan"
    source_url: str = "https://coronavirus.uz/uz/lists"
    regex: dict = {
        "total_vaccinations_people_vaccinated": (
            r"Айни кунгача мамлакатимизда жами ([\d\s,]+) доза вакцинадан фойдаланилди\. "
            r"Улардан:\n1-босқич эмланганлар ([\d\s,]+) нафарни"
        ),
        "people_fully_vaccinated": r"Шу кунгача тўлиқ эмланган фуқаролар ([\d\s]+) нафарни ташкил қилмоқда",
    }

    def read(self, last_update):
        data = []
        with get_driver() as driver:
            driver.get(self.source_url)
            self._click_detail_buttons(driver)
            elems = self._get_elems(driver)
            for elem in elems:
                data_ = self._parse_data(elem)
                if data_:
                    data.append(data_)
        df = pd.DataFrame(data)
        return df[df.date > last_update]

    def _click_detail_buttons(self, driver):
        buttons = driver.find_element_by_id("accordionExample").find_elements_by_tag_name("button")
        for btn in buttons:
            btn.click()

    def _get_elems(self, driver):
        # Get elements
        elems = driver.find_elements_by_class_name("card-body")
        elems = [e for e in elems if e.text != ""]
        return elems

    def _parse_date(self, elem):
        dt = elem.find_element_by_xpath("../..").find_element_by_class_name("cdate").text
        return clean_date(dt, "%d.%m.%Y")

    def _parse_data(self, elem):
        match_1 = re.search(self.regex["total_vaccinations_people_vaccinated"], elem.text)
        match_2 = re.search(self.regex["people_fully_vaccinated"], elem.text)
        if match_1 is None:
            return None
        else:
            return {
                "date": self._parse_date(elem),
                "total_vaccinations": clean_count(match_1.group(1)),
                "people_vaccinated": clean_count(match_1.group(2)),
                "people_fully_vaccinated": clean_count(match_2.group(1)) if match_2 else None,
            }

    def pipe_metadata(self, df):
        return df.assign(
            location=self.location,
            source_url=self.source_url,
        )

    def pipe_vaccine(self, df):
        return df.assign(vaccine="Oxford/AstraZeneca, Sputnik V, ZF2001")

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            if date < "2021-05-10":
                return "Oxford/AstraZeneca, ZF2001"
            elif "2021-05-10" <= date < "2021-09-30":
                return "Oxford/AstraZeneca, Sputnik V, ZF2001"
            return "Oxford/AstraZeneca, Moderna, Sputnik V, ZF2001"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipeline(self, df):
        return df.pipe(self.pipe_metadata).pipe(self.pipe_vaccine)

    def export(self):
        output_file = paths.out_vax(self.location)
        last_update = pd.read_csv(output_file).date.max()
        df = self.read(last_update).pipe(self.pipeline)
        df = merge_with_current_data(df, output_file)
        df.to_csv(output_file, index=False)


def main():
    Uzbekistan().export()
