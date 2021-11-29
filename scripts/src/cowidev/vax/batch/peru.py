import pandas as pd

from cowidev.utils.clean.dates import localdatenow
from cowidev.vax.utils.files import export_metadata_age
from cowidev.utils import paths


class Peru:
    def __init__(self) -> None:
        self.location = "Peru"
        self.source_url = (
            "https://github.com/jmcastagnetto/covid-19-peru-vacunas/raw/main/datos/vacunas_covid_resumen.csv"
        )
        self.source_url_age = (
            "https://github.com/jmcastagnetto/covid-19-peru-vacunas/raw/main/datos/vacunas_covid_rangoedad_owid.csv"
        )
        self.source_url_ref = "https://www.datosabiertos.gob.pe/dataset/vacunacion"
        self.vaccine_mapping = {
            "SINOPHARM": "Sinopharm/Beijing",
            "PFIZER": "Pfizer/BioNTech",
            "ASTRAZENECA": "Oxford/AstraZeneca",
        }

    def read(self):
        return pd.read_csv(
            self.source_url,
            usecols=["fecha_vacunacion", "fabricante", "dosis", "n_reg"],
        )

    def read_age(self):
        return pd.read_csv(self.source_url_age)

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={"fecha_vacunacion": "date", "fabricante": "vaccine"})
        return df.dropna(subset=["vaccine"])

    def pipe_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        # Check vaccine names
        unknown_vaccines = set(df["vaccine"].unique()).difference(self.vaccine_mapping.keys())
        if unknown_vaccines:
            raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))
        # Check dose number
        dose_num_wrong = set(df.dosis).difference({1, 2, 3})
        if dose_num_wrong:
            raise ValueError(f"Invalid dose number. Check field `dosis`: {dose_num_wrong}")
        return df

    def pipe_rename_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.replace(self.vaccine_mapping)

    def pipe_format(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.drop(columns="vaccine")
            .groupby(["date", "dosis"], as_index=False)
            .sum()
            .pivot(index="date", columns="dosis", values="n_reg")
            .rename(columns={1: "people_vaccinated", 2: "people_fully_vaccinated", 3: "total_boosters"})
            .fillna(0)
            .sort_values("date")
            .cumsum()
            .reset_index()
        )

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated + df.total_boosters)

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            vaccine=", ".join(sorted(self.vaccine_mapping.values())),
            source_url=self.source_url_ref,
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_checks)
            .pipe(self.pipe_rename_vaccines)
            .pipe(self.pipe_format)
            .pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_metadata)
        )

    def pipe_age_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        # print(df.columns)
        if (df.people_vaccinated_per_hundred > 100).sum():
            raise ValueError("Check `people_vaccinated_per_hundred` field! Found values above 100%.")
        if (df.people_fully_vaccinated_per_hundred > 100).sum():
            raise ValueError("Check `people_fully_vaccinated_per_hundred` field! Found values above 100%.")
        if (df.sunday.min() < "2021-02-08") or (df.sunday.max() > localdatenow("America/Lima")):
            raise ValueError("Check `sunday` field! Some dates may be out of normal")
        if not (df.location.unique() == "Peru").all():
            raise ValueError("Invalid values in `location` field!")
        return df

    def pipe_age_rename_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={"sunday": "date"})

    def pipeline_age(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_age_checks).pipe(self.pipe_age_rename_date)

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)
        # Age data
        df_age = self.read_age().pipe(self.pipeline_age)
        df_age.to_csv(paths.out_vax(self.location, age=True), index=False)
        export_metadata_age(
            df_age,
            "Ministerio de Salud via https://github.com/jmcastagnetto/covid-19-peru-vacunas",
            self.source_url_ref,
        )


def main():
    Peru().export()
