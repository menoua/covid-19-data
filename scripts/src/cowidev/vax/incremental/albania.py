import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import increment, enrich_data


class Albania:
    def __init__(self):
        self.location = "Albania"
        self.source_url = "https://shendetesia.gov.al/category/lajme/page"
        self._num_max_pages = 3
        self.regex = {
            "title": r"Vaksinimi antiCOVID\/ Kryhen [0-9,]+ vaksinime",
            "date": r"Postuar më: (\d{1,2}\/\d{1,2}\/202\d)",
            "people_vaccinated": r"([\d,]+) doza të para",
            "people_fully_vaccinated": r"([\d,]+) doza të dyta",
            "total_boosters": r"([\d,]+) doza të treta",
        }

    def read(self):
        for cnt in range(1, self._num_max_pages + 1):
            url = f"{self.source_url}/{cnt}/"
            soup = get_soup(url, verify=False)
            data, proceed = self.parse_data(soup)
            if not proceed:
                break
        return pd.Series(data)

    def parse_data(self, soup: BeautifulSoup) -> tuple:
        elem = self.get_last_element(soup)
        if elem is None:
            return None, True
        soup = get_soup(elem["link"], verify=False)
        record = {
            "source_url": elem["link"],
            "date": elem["date"],
            **self.parse_data_news_page(soup),
        }
        return record, False

    def get_last_element(self, soup: BeautifulSoup) -> list:
        last = soup.find(id="leftContent").find("h2", text=re.compile(self.regex["title"]))
        if len(last) == 0:
            return None
        last = {"link": last.a.get("href"), "date": self.parse_date(last)}
        return last

    def parse_date(self, elem):
        match = re.search(self.regex["date"], elem.parent.text)
        return clean_date(match.group(1), "%d/%m/%Y", minus_days=1)

    def parse_data_news_page(self, soup: BeautifulSoup):
        people_vaccinated = re.search(self.regex["people_vaccinated"], soup.text)
        people_fully_vaccinated = re.search(self.regex["people_fully_vaccinated"], soup.text)
        total_boosters = re.search(self.regex["total_boosters"], soup.text)
        return {
            "people_vaccinated": clean_count(people_vaccinated.group(1)),
            "people_fully_vaccinated": clean_count(people_fully_vaccinated.group(1)),
            "total_boosters": clean_count(total_boosters.group(1)),
        }

    def pipe_total_vaccinations(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds, "total_vaccinations", ds.people_vaccinated + ds.people_fully_vaccinated + ds.total_boosters
        )

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac, Sputnik V")

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_total_vaccinations).pipe(self.pipe_location).pipe(self.pipe_vaccine)

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
    Albania().export()


if __name__ == "__main__":
    main()
