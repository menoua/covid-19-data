import pandas as pd

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web import request_json
from cowidev.vax.utils.incremental import enrich_data, increment


class FaeroeIslands:
    location: str = "Faeroe Islands"
    source_url: str = "https://corona.fo/json/stats"
    source_url_ref: str = "https://corona.fo/api"

    def read(self) -> pd.Series:
        data = request_json(self.source_url)["stats"]
        return pd.DataFrame.from_records(data).iloc[0]

    def pipe_metrics(self, ds: pd.Series) -> pd.Series:
        ds = enrich_data(ds, "people_vaccinated", clean_count(ds["first_vaccine_number"]))
        ds = enrich_data(ds, "people_fully_vaccinated", clean_count(ds["second_vaccine_number"]))
        total_vaccinations = ds["people_vaccinated"] + ds["people_fully_vaccinated"]
        return enrich_data(ds, "total_vaccinations", total_vaccinations)

    def pipe_format_date(self, ds: pd.Series) -> pd.Series:
        date = localdate("Atlantic/Faeroe")
        return enrich_data(ds, "date", date)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Moderna, Pfizer/BioNTech")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url_ref)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds.pipe(self.pipe_metrics)
            .pipe(self.pipe_format_date)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_source)
        )

    def export(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=str(data["location"]),
            total_vaccinations=int(data["total_vaccinations"]),
            people_vaccinated=int(data["people_vaccinated"]),
            people_fully_vaccinated=int(data["people_fully_vaccinated"]),
            date=str(data["date"]),
            source_url=str(data["source_url"]),
            vaccine=str(data["vaccine"]),
        )


def main():
    FaeroeIslands().export()


if __name__ == "__main__":
    main()
