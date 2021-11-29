import pandas as pd

from cowidev.utils.clean.dates import localdate
from cowidev.utils.web import request_json
from cowidev.vax.utils.incremental import enrich_data, increment


class India:

    location: str = "India"
    source_name: str = "cowin"  # mohfw, cowin
    source_url_ref: str = {
        "mohfw": "https://www.mohfw.gov.in/",
        "cowin": "https://dashboard.cowin.gov.in/",
    }

    @property
    def source_url(self):
        return {
            "mohfw": "https://www.mygov.in/sites/default/files/covid/vaccine/vaccine_counts_today.json",
            "cowin": (
                "https://api.cowin.gov.in/api/v1/reports/v2/getPublicReports?state_id=&district_id=&date="
                f"{self.date_str}"
            ),
        }

    def read(self):
        data = request_json(self.source_url[self.source_name])
        if self.source_name == "mohfw":
            return self.read_mohfw(data)
        elif self.source_name == "cowin":
            return self.read_cowin(data)
        raise ValueError(f"Not valid class attribute `source_name`: {self.source_name}")

    def read_cowin(self, json_data) -> pd.Series:
        data = json_data["topBlock"]["vaccination"]

        people_vaccinated = data["tot_dose_1"]
        people_fully_vaccinated = data["tot_dose_2"]
        total_vaccinations = data["total"]

        return pd.Series(
            {
                "date": self.date_str,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "total_vaccinations": total_vaccinations,
            }
        )

    def read_mohfw(self, json_data) -> pd.Series:
        people_vaccinated = json_data["india_dose1"]
        people_fully_vaccinated = json_data["india_dose2"]
        total_vaccinations = json_data["india_total_doses"]
        date = json_data["day"]

        return pd.Series(
            {
                "date": date,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "total_vaccinations": total_vaccinations,
            }
        )

    @property
    def date_str(self):
        return localdate("Asia/Calcutta", force_today=True)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Covaxin, Oxford/AstraZeneca, Sputnik V")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url_ref[self.source_name])

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
    India().export()
