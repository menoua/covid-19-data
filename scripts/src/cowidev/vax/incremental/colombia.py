import re

import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.utils.incremental import enrich_data, increment


class Colombia:
    def __init__(self, gsheets_api) -> None:
        self.location = "Colombia"
        self.source_url = "https://docs.google.com/spreadsheets/d/1eblBeozGn1soDGXbOIicwyEDkUqNMzzpJoAKw84TTA4"
        self.gsheets_api = gsheets_api

    @property
    def sheet_id(self):
        return self.source_url.split("/")[-1]

    def read(self) -> pd.Series:
        ws = self.gsheets_api.get_worksheet(self.sheet_id, "Reporte diario")
        df = self._parse_data(ws)
        return df

    def _parse_data(self, worksheet):

        for row in worksheet.values():
            for value in row:
                if "Total dosis aplicadas al " in str(value):
                    total_vaccinations = row[-1]
                    if type(total_vaccinations) != int:
                        total_vaccinations = clean_count(total_vaccinations)
                    date_raw = re.search(r"[\d-]{10}$", value).group(0)
                    date_str = clean_date(date_raw, "%d-%m-%Y")
                elif value == "Esquemas completos segundas + únicas dosis":
                    people_fully_vaccinated = row[-1]
                    if type(people_fully_vaccinated) != int:
                        people_fully_vaccinated = clean_count(people_fully_vaccinated)
                elif value == "Total únicas dosis acumuladas":
                    unique_doses = row[-1]
                    if type(unique_doses) != int:
                        unique_doses = clean_count(unique_doses)

        if total_vaccinations is None or people_fully_vaccinated is None:
            raise ValueError("Date is not where it is expected be! Check worksheet")
        return pd.Series(
            {
                "date": date_str,
                "total_vaccinations": total_vaccinations,
                "people_fully_vaccinated": people_fully_vaccinated,
                "people_vaccinated": total_vaccinations - people_fully_vaccinated + unique_doses,
            }
        )

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", "Colombia")

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

    def export(self):
        data = self.read()
        if "total_vaccinations" in data:
            data = data.pipe(self.pipeline)
            increment(
                location=data["location"],
                total_vaccinations=data["total_vaccinations"],
                people_vaccinated=data["people_vaccinated"],
                people_fully_vaccinated=data["people_fully_vaccinated"],
                date=data["date"],
                source_url=data["source_url"],
                vaccine=data["vaccine"],
            )
        else:
            print("skipped")


def main(gsheets_api):
    Colombia(gsheets_api=gsheets_api).export()
