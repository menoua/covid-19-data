import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.utils.web import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Singapore:
    def __init__(self) -> None:
        self.location = "Singapore"
        self.feed_url = "https://www.moh.gov.sg/feeds/news-highlights"

    def find_article(self) -> str:
        soup = get_soup(self.feed_url)
        for link in soup.find_all("item"):
            elements = link.children
            for elem in elements:
                if "local-covid-19-situation" in elem:
                    return elem

    def read(self) -> pd.Series:
        self.source_url = self.find_article()
        soup = get_soup(self.source_url)
        return pd.Series(data=self.parse_text(soup))

    def parse_text(self, soup: BeautifulSoup) -> pd.Series:
        # Summary figures
        date, share_fully_vaccinated, share_vaccinated, _ = self._parse_text_summary(soup)
        # National figures
        national_doses, national_boosters, national_people_vaccinated = self._parse_text_national(soup)
        # WHO
        who_doses, who_people_vaccinated = self._parse_text_who(soup)
        # Combine
        total_vaccinations = national_doses + who_doses
        people_vaccinated = national_people_vaccinated + who_people_vaccinated
        people_fully_vaccinated = round(people_vaccinated * (share_fully_vaccinated / share_vaccinated))

        return {
            "date": date,
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "total_boosters": national_boosters,
        }

    def _parse_text_summary(self, soup):
        preamble = (
            r"As of ([\d]+ [A-Za-z]+ 20\d{2}), (\d+)% of our population has completed their full regimen/"
            r" received two doses of COVID-19 vaccines, (\d+)% has received at least one dose,"
            r" and (\d+)% ha(?:ve|s) received (?:their )?booster(?:s)?"
        )
        data = re.search(preamble, soup.text).groups()
        date = clean_date(data[0], fmt="%d %B %Y", lang="en")
        share_fully_vaccinated = clean_count(data[1])
        share_vaccinated = clean_count(data[2])
        share_boosters = clean_count(data[3])
        return date, share_fully_vaccinated, share_vaccinated, share_boosters

    def _parse_text_national(self, soup):
        national_program = (
            r"We have administered a total of ([\d,]+) doses of COVID-19 vaccines under the.*"
            r"In total, ([\d,]+) individuals have received at least one dose of vaccine under the national vaccination"
            r" programme,.* ([\d,]+) (?:individuals )?have (?:received|taken) their booster shots"
        )
        data = re.search(national_program, soup.text).groups()
        national_doses = clean_count(data[0])
        national_people_vaccinated = clean_count(data[1])
        national_boosters = clean_count(data[2])
        return national_doses, national_boosters, national_people_vaccinated

    def _parse_text_who(self, soup):
        who_eul = (
            r"In addition, ([\d,]+) doses of other vaccines recognised in the World Health Organization.s Emergency"
            r" Use Listing \(WHO EUL\) have been administered, covering ([\d,]+) individuals\."
        )
        data = re.search(who_eul, soup.text).groups()
        who_doses = clean_count(data[0])
        who_people_vaccinated = clean_count(data[1])
        return who_doses, who_people_vaccinated

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", "Singapore")

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Moderna, Pfizer/BioNTech, Sinovac")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_source).pipe(self.pipe_vaccine)

    def export(self):
        data = self.read().pipe(self.pipeline).to_dict()
        increment(**data)


def main():
    Singapore().export()
