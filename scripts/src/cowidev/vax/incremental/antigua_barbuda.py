import pandas as pd

from cowidev.utils.clean import clean_count, extract_clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import increment, enrich_data


class AntiguaBarbuda:
    def __init__(self, source_url: str, location: str):
        self.source_url = source_url
        self.location = location
        self.regex = {"date": r"\[Updated on ([a-zA-Z]+ \d{1,2}, 202\d)\]"}

    def read(self) -> pd.DataFrame:
        soup = get_soup(self.source_url)
        return self.parse_data(soup)

    def parse_data(self, soup):
        dose1_elem, dose2_elem = self._get_elements(soup)
        return pd.Series(
            {
                "date": self._parse_date(dose1_elem, dose2_elem),
                "people_vaccinated": self._parse_metric(dose1_elem),
                "people_fully_vaccinated": self._parse_metric(dose2_elem),
            }
        )

    def _get_elements(self, soup):
        # Get elements
        h1 = soup.find_all("h1")
        for h in h1:
            text = h.text.strip()
            if text == "Vaccinated Cases 1st Dose":
                dose1_elem = h.parent
            if text == "Vaccinated Cases 2nd Dose":
                dose2_elem = h.parent
        return dose1_elem, dose2_elem

    def _parse_date(self, dose1_elem, dose2_elem):
        date1_raw = dose1_elem.find("h2").text
        date1 = extract_clean_date(date1_raw, self.regex["date"], "%B %d, %Y", minus_days=1, lang="en")
        date2_raw = dose2_elem.find("h2").text
        date2 = extract_clean_date(date2_raw, self.regex["date"], "%B %d, %Y", minus_days=1, lang="en")
        if date1 == date2:
            return date1
        raise ValueError("Dates in first and second doses are not aligned")

    def _parse_metric(self, elem):
        elems = elem.find_all("div")
        for elem in elems:
            if "Total Vaccinated" in elem.text:
                return clean_count(elem.find(class_="case-Number").text)

    def pipe_people_vaccinated(self, ds: pd.Series) -> pd.Series:
        total_vaccinations = ds.loc["people_vaccinated"] + ds.loc["people_fully_vaccinated"]
        return enrich_data(ds, "total_vaccinations", total_vaccinations)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sputnik V")

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
    AntiguaBarbuda(
        source_url="https://covid19.gov.ag",
        location="Antigua and Barbuda",
    ).to_csv()


if __name__ == "__main__":
    main()
