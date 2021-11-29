import pandas as pd

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Georgia:
    def __init__(self):
        self.source_url = "https://vaccines.ncdc.ge/vaccinationprocess/"
        self.location = "Georgia"

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url, verify=False)
        return self.parse_data(soup)

    def parse_data(self, soup):
        widgets = soup.find_all(class_="textwidget")
        total_vaccinations = clean_count(widgets[0].text)
        people_fully_vaccinated = clean_count(widgets[1].text)
        people_vaccinated = total_vaccinations - people_fully_vaccinated
        return pd.Series(
            {
                "total_vaccinations": total_vaccinations,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "date": localdate("Asia/Tbilisi"),
            }
        )

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinovac",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

    def export(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    Georgia().export()


if __name__ == "__main__":
    main()
