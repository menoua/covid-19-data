import datetime
import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import merge_with_current_data
from cowidev.utils import paths


class Monaco:
    def __init__(self, source_url: str, location: str):
        self.source_url = source_url
        self.location = location
        self._num_max_pages = 5
        self._base_url = "https://www.gouv.mc"
        self.regex = {
            "title": r"Covid-19 : .*",
            "people_vaccinated": r"Nombre de personnes vaccinées en primo injection\s:\s([\d\.]+)",
            "people_fully_vaccinated": r"Nombre de personnes ayant reçu l’injection de rappel\s:\s([\d\.]+)",
            "date": r"voici les chiffres arrêtés au (\d+ \w+) inclus",
        }

    def read(self, last_update: str) -> pd.DataFrame:
        data = []
        for cnt in range(0, 5 * self._num_max_pages, 5):
            # print(f"page: {cnt}")
            url = f"{self.source_url}/(offset)/{cnt}/"
            soup = get_soup(url)
            data_, proceed = self.parse_data(soup, last_update)
            data.extend(data_)
            if not proceed:
                break
        return pd.DataFrame(data)

    def parse_data(self, soup: BeautifulSoup, last_update: str) -> tuple:
        elems = self.get_elements(soup)
        records = []
        for elem in elems:
            if elem["date"] > last_update:
                # print(elem["date"], elem)
                soup = get_soup(elem["link"])
                record = {
                    "source_url": elem["link"],
                    **self.parse_data_news_page(soup),
                }
                records.append(record)
            else:
                # print(elem["date"], "END")
                return records, False
        return records, True

    def get_elements(self, soup: BeautifulSoup) -> list:
        elems = soup.find_all("h3", text=re.compile(self.regex["title"]))
        elems = [{"link": self.parse_link(elem), "date": self.parse_date(elem)} for elem in elems]
        return elems

    def parse_data_news_page(self, soup: BeautifulSoup):
        people_vaccinated = re.search(self.regex["people_vaccinated"], soup.text)
        people_fully_vaccinated = re.search(self.regex["people_fully_vaccinated"], soup.text)
        date = re.search(self.regex["date"], soup.text)
        metrics = {}
        if people_vaccinated:
            metrics["people_vaccinated"] = clean_count(people_vaccinated.group(1))
        if people_fully_vaccinated:
            metrics["people_fully_vaccinated"] = clean_count(people_fully_vaccinated.group(1))
        if date:
            metrics["date"] = clean_date(
                date.group(1) + " " + str(datetime.date.today().year),
                fmt="%d %B %Y",
                lang="fr",
            )
        return metrics

    def parse_date(self, elem):
        date_raw = elem.parent.find(class_="date").text
        return clean_date(date_raw, "%d %B %Y", minus_days=1, loc="fr_FR")

    def parse_link(self, elem):
        href = elem.a.get("href")
        return f"{self._base_url}/{href}"

    def pipe_filter_nans(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(subset=["people_vaccinated", "people_fully_vaccinated"])

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated)

    def pipe_drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values("date").drop_duplicates(
            subset=[
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
            ],
            keep="first",
        )

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Pfizer/BioNTech")

    def pipe_select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
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
        ]

    def pipeline(self, df: pd.Series) -> pd.Series:
        return (
            df.pipe(self.pipe_filter_nans)
            .pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_drop_duplicates)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_select_output_columns)
            .sort_values(by="date")
        )

    def to_csv(self):
        """Generalized."""
        output_file = paths.out_vax(self.location)
        last_update = pd.read_csv(output_file).date.max()
        df = self.read(last_update)
        if not df.empty and "people_vaccinated" in df.columns:
            df = df.pipe(self.pipeline)
            df = merge_with_current_data(df, output_file)
            df = df.pipe(self.pipe_drop_duplicates)
            df.to_csv(output_file, index=False)


def main():
    Monaco(
        source_url="https://www.gouv.mc/Action-Gouvernementale/Coronavirus-Covid-19/Actualites/",
        location="Monaco",
    ).to_csv()


if __name__ == "__main__":
    main()
