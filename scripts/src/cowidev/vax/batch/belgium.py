import pandas as pd
from cowidev.utils import paths


class Belgium:
    def __init__(self) -> None:
        self.location = "Belgium"
        self.source_url = "https://epistat.sciensano.be/Data/COVID19BE_VACC.csv"
        self.source_url_ref = "https://epistat.wiv-isp.be/covid/"

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.source_url, usecols=["DATE", "DOSE", "COUNT"])

    def pipe_dose_check(self, df: pd.DataFrame) -> pd.DataFrame:
        doses_wrong = set(df.DOSE).difference(["A", "B", "C", "E"])
        if doses_wrong:
            raise ValueError(f"Invalid dose type {doses_wrong}")
        return df

    def pipe_aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(["DATE", "DOSE"], as_index=False)
            .sum()
            .sort_values("DATE")
            .pivot(index="DATE", columns="DOSE", values="COUNT")
            .reset_index()
            .fillna(0)
        )

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "DATE": "date",
            }
        )

    def pipe_add_totals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(
            total_vaccinations=df.A + df.B + df.C,
            people_vaccinated=df.A + df.C,
            people_fully_vaccinated=df.B + df.C,
            total_boosters=df.E,
        )
        return df.drop(columns=["A", "B", "C", "E"])

    def pipe_cumsum(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df.total_vaccinations.cumsum().astype(int),
            people_vaccinated=df.people_vaccinated.cumsum().astype(int),
            people_fully_vaccinated=df.people_fully_vaccinated.cumsum().astype(int),
            total_boosters=df.total_boosters.cumsum().astype(int),
        )

    def pipe_vaccine_name(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine_name(date: str) -> str:
            # See timeline in:
            # https://datastudio.google.com/embed/u/0/reporting/c14a5cfc-cab7-4812-848c-0369173148ab/page/hOMwB
            if date < "2021-01-11":
                return "Pfizer/BioNTech"
            elif "2021-01-11" <= date < "2021-02-12":
                return "Moderna, Pfizer/BioNTech"
            elif "2021-02-12" <= date < "2021-04-28":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            elif "2021-04-28" <= date:
                return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url_ref)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_dose_check)
            .pipe(self.pipe_aggregate)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_add_totals)
            .pipe(self.pipe_cumsum)
            .pipe(self.pipe_vaccine_name)
            .pipe(self.pipe_metadata)
        )

    def export(self):
        (self.read().pipe(self.pipeline).to_csv(paths.out_vax(self.location), index=False))


def main():
    Belgium().export()
