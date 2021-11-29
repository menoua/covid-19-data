import pandas as pd
from cowidev.utils import paths


class Norway:
    def __init__(self) -> None:
        self.location = "Norway"
        self.source_url = "https://raw.githubusercontent.com/folkehelseinstituttet/surveillance_data/master/covid19/data_covid19_sysvak_by_time_location_latest.csv"
        self.source_url_ref = "https://github.com/folkehelseinstituttet/surveillance_data"

    def read(self):
        return pd.read_csv(self.source_url)

    def pipe_filter_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df.granularity_geo == "nation"]

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[["date", "cum_n_dose_1", "cum_n_dose_2", "cum_n_dose_3_all"]].rename(
            columns={
                "cum_n_dose_1": "people_vaccinated",
                "cum_n_dose_2": "people_fully_vaccinated",
                "cum_n_dose_3_all": "total_boosters",
            }
        )

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str):
            if date < "2021-01-15":
                return "Pfizer/BioNTech"
            elif "2021-01-15" <= date < "2021-02-10":
                return "Moderna, Pfizer/BioNTech"
            elif "2021-02-10" <= date < "2021-03-11":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            elif "2021-03-11" <= date:
                return "Moderna, Pfizer/BioNTech"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df.people_vaccinated.fillna(0)
            + df.people_fully_vaccinated.fillna(0)
            + df.total_boosters.fillna(0)
        )

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            source_url=self.source_url_ref,
            location=self.location,
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_filter_rows)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_metadata)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)


def main():
    Norway().export()
