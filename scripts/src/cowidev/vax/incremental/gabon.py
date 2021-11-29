import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_count, extract_clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import increment, enrich_data


class Gabon:
    def __init__(self, source_url: str, location: str):
        self.source_url = source_url
        self.location = location
        self.regex = {"date": r"(\d{1,2}-\d{1,2}-202\d) \d{1,2}:\d{1,2}:\d{1,2}"}

    def read(self) -> pd.DataFrame:
        soup = get_soup(self.source_url)
        return self.parse_data(soup)

    def parse_data(self, soup: BeautifulSoup) -> pd.Series:
        h6 = soup.find_all("h6")
        for i, h in enumerate(h6):
            # print(i)
            text = h.text.strip()
            if text == "1ière dose":
                people_vaccinated = clean_count(h.parent.find("h3").text)
            elif text == "2ième dose":
                people_fully_vaccinated = clean_count(h.parent.find("h3").text)
            else:
                match = re.search(self.regex["date"], text)
                if match:
                    date_str = extract_clean_date(text, self.regex["date"], "%d-%m-%Y")
        return pd.Series(
            {
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "date": date_str,
            }
        )

    def pipe_people_vaccinated(self, ds: pd.Series) -> pd.Series:
        total_vaccinations = ds.loc["people_vaccinated"] + ds.loc["people_fully_vaccinated"]
        return enrich_data(ds, "total_vaccinations", total_vaccinations)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Sinopharm/Beijing, Sputnik V")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, df: pd.Series) -> pd.Series:
        return (
            df.pipe(self.pipe_people_vaccinated)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_source)
        )

    def to_csv(self):
        """Generalized."""
        ds = self.read().pipe(self.pipeline)
        increment(
            location=ds["location"],
            total_vaccinations=ds["total_vaccinations"],
            people_vaccinated=ds["people_vaccinated"],
            people_fully_vaccinated=ds["people_fully_vaccinated"],
            date=ds["date"],
            source_url=ds["source_url"],
            vaccine=ds["vaccine"],
        )


def main():
    Gabon(
        source_url="https://monitoring-covid19gabon.ga/",
        location="Gabon",
    ).to_csv()


if __name__ == "__main__":
    main()
