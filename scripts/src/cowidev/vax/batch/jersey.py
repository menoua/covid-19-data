import requests
import tempfile
import re

import pandas as pd

from cowidev.vax.utils.files import export_metadata_age
from cowidev.utils import paths


class Jersey:
    def __init__(self):
        """Constructor.

        Args:
            source_url (str): Source data url
            location (str): Location name
            columns_rename (dict, optional): Maps original to new names. Defaults to None.
        """
        self.source_url = "https://www.gov.je/Datasets/ListOpenData?ListName=COVID19Weekly&clean=true"
        self.location = "Jersey"
        self.columns_rename = {
            "Date": "date",
            "VaccinationsTotalNumberDoses": "total_vaccinations",
            "VaccinationsTotalNumberFirstDoseVaccinations": "people_vaccinated",
            "VaccinationsTotalNumberSecondDoseVaccinations": "people_fully_vaccinated",
        }

    def read(self):
        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, mode="wb") as f:
                f.write(requests.get(self.source_url).content)
            return pd.read_csv(tf.name)

    def pipe_select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.columns_rename.keys()]

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.columns_rename.keys()].rename(columns=self.columns_rename)

    def pipe_enrich_vaccine_name(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            if date >= "2021-04-07":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            return "Oxford/AstraZeneca, Pfizer/BioNTech"

        return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))

    def pipe_enrich_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_select_columns)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_enrich_vaccine_name)
            .pipe(self.pipe_enrich_columns)
            .sort_values("date")[
                [
                    "location",
                    "date",
                    "vaccine",
                    "source_url",
                    "total_vaccinations",
                    "people_vaccinated",
                    "people_fully_vaccinated",
                ]
            ]
        )

    def pipe_age_select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            [
                "Date",
                "VaccinationsPercentagePopulationVaccinatedFirstDose80yearsandover",
                "VaccinationsPercentagePopulationVaccinatedFirstDose75to79years",
                "VaccinationsPercentagePopulationVaccinatedFirstDose70to74years",
                "VaccinationsPercentagePopulationVaccinatedFirstDose65to69years",
                "VaccinationsPercentagePopulationVaccinatedFirstDose60to64years",
                "VaccinationsPercentagePopulationVaccinatedFirstDose55to59years",
                "VaccinationsPercentagePopulationVaccinatedFirstDose50to54years",
                "VaccinationsPercentagePopulationVaccinatedFirstDose40to49years",
                "VaccinationsPercentagePopulationVaccinatedFirstDose30to39years",
                "VaccinationsPercentagePopulationVaccinatedFirstDose18to29years",
                "VaccinationsPercentagePopulationVaccinatedFirstDose17yearsandunder",
                "VaccinationsPercentagePopulationVaccinatedSecondDose80yearsandover",
                "VaccinationsPercentagePopulationVaccinatedSecondDose75to79years",
                "VaccinationsPercentagePopulationVaccinatedSecondDose70to74years",
                "VaccinationsPercentagePopulationVaccinatedSecondDose65to69years",
                "VaccinationsPercentagePopulationVaccinatedSecondDose60to64years",
                "VaccinationsPercentagePopulationVaccinatedSecondDose55to59years",
                "VaccinationsPercentagePopulationVaccinatedSecondDose50to54years",
                "VaccinationsPercentagePopulationVaccinatedSecondDose40to49years",
                "VaccinationsPercentagePopulationVaccinatedSecondDose30to39years",
                "VaccinationsPercentagePopulationVaccinatedSecondDose18to29years",
                "VaccinationsPercentagePopulationVaccinatedSecondDose17yearsandunder",
            ]
        ]

    def _extract_age_group(self, age_group_raw):
        regex_17 = r"VaccinationsPercentagePopulationVaccinated(?:First|Second)Dose17yearsandunder"
        regex_80 = r"VaccinationsPercentagePopulationVaccinated(?:First|Second)Dose80yearsandover"
        regex = r"VaccinationsPercentagePopulationVaccinated(?:First|Second)Dose(\d+)to(\d+)years"
        if re.match(regex_17, age_group_raw):
            age_group = "0-17"
        elif re.match(regex_80, age_group_raw):
            age_group = "80-"
        elif re.match(regex, age_group_raw):
            age_group = "-".join(re.match(regex, age_group_raw).group(1, 2))
        return age_group

    def pipe_age_create_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        # Split data in dataframes with first and second doses
        df1 = df.filter(regex=r"Date|VaccinationsPercentagePopulationVaccinatedFirstDose.*")
        df2 = df.filter(regex=r"Date|VaccinationsPercentagePopulationVaccinatedSecondDose.*")
        # Melt dataframes
        df1 = df1.melt(
            id_vars="Date",
            var_name="age_group",
            value_name="people_vaccinated_per_hundred",
        )
        df2 = df2.melt(
            id_vars="Date",
            var_name="age_group",
            value_name="people_fully_vaccinated_per_hundred",
        )
        # Process and merge dataframes
        df1 = df1.assign(age_group=df1.age_group.apply(self._extract_age_group))
        df2 = df2.assign(age_group=df2.age_group.apply(self._extract_age_group))
        df = df1.merge(df2, on=["Date", "age_group"])
        return df

    def pipe_age_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={"Date": "date"})

    def pipe_age_minmax_values(self, df: pd.DataFrame) -> pd.DataFrame:
        df[["age_group_min", "age_group_max"]] = df.age_group.str.split("-", expand=True)
        return df

    def pipe_metrics_scale_100(self, df: pd.DataFrame) -> pd.DataFrame:
        column_metrics = [
            "people_vaccinated_per_hundred",
            "people_fully_vaccinated_per_hundred",
        ]
        df[column_metrics] = (df[column_metrics] * 100).round(2)
        return df

    def pipeline_age(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_age_select_columns)
            .pipe(self.pipe_age_create_groups)
            .pipe(self.pipe_age_rename_columns)
            .pipe(self.pipe_age_minmax_values)
            .pipe(self.pipe_enrich_columns)
            .pipe(self.pipe_metrics_scale_100)
            .sort_values(["date", "age_group_min"])[
                [
                    "location",
                    "date",
                    "age_group_min",
                    "age_group_max",
                    "people_vaccinated_per_hundred",
                    "people_fully_vaccinated_per_hundred",
                ]
            ]
        )

    def to_csv(self):
        """Generalized."""
        df_base = self.read()
        # Main data
        df = df_base.pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)
        # Age data
        df_age = df_base.pipe(self.pipeline_age)
        df_age.to_csv(paths.out_vax(self.location, age=True), index=False)
        export_metadata_age(df_age, "Government of Jersey", self.source_url)


def main():
    Jersey().to_csv()


if __name__ == "__main__":
    main()
