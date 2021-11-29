import re
import pandas as pd

from cowidev.vax.utils.files import export_metadata_age, export_metadata_manufacturer
from cowidev.utils import paths


vaccines_mapping = {
    "total_coronavac": "Sinovac",
    "total_pfizer": "Pfizer/BioNTech",
    "total_astrazeneca": "Oxford/AstraZeneca",
}


class Uruguay:
    def __init__(self):
        self.source_url = "https://raw.githubusercontent.com/3dgiordano/covid-19-uy-vacc-data/main/data/Uruguay.csv"
        self.source_url_age = "https://raw.githubusercontent.com/3dgiordano/covid-19-uy-vacc-data/main/data/Age.csv"
        self.location = "Uruguay"

    def read(self):
        # Load main data
        df = pd.read_csv(self.source_url)
        # Load age data
        regex = r"(date|coverage_(people|fully)_\d+_\d+)"
        df_age = df_age = pd.read_csv(self.source_url_age, usecols=lambda x: re.match(regex, x))
        return df, df_age

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:

        # Remove rows that prevent people_vaccinated from increasing monotonically
        last_people_vaccinated = df.sort_values("date").people_vaccinated.values[-1]
        df = df[df.people_vaccinated <= last_people_vaccinated]

        return df[
            [
                "location",
                "date",
                "vaccine",
                "source_url",
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
                "total_boosters",
            ]
        ]

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.drop(columns=["total_vaccinations"])
            .melt(
                id_vars=["date", "location"],
                value_vars=["total_coronavac", "total_pfizer", "total_astrazeneca"],
                var_name="vaccine",
                value_name="total_vaccinations",
            )
            .replace(vaccines_mapping)
            .sort_values(["date", "vaccine"])
        )

    def pipe_age_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        age_groups_accepted = {
            "12_17",
            "18_24",
            "25_34",
            "35_44",
            "45_54",
            "55_64",
            "65_74",
            "75_115",
        }
        age_groups = set(df.columns.str.extract(r"coverage_(?:people|fully)_(.*)", expand=False).dropna())
        age_groups_wrong = age_groups.difference(age_groups_accepted)
        if age_groups_wrong:
            raise ValueError(f"Invalid age groups: {age_groups_wrong}")
        return df

    def pipe_age_melt_pivot(self, df: pd.DataFrame) -> pd.DataFrame:
        # Melt
        df = df.melt(id_vars=["date"])
        # Assign metric to each entry
        df = df.assign(
            metric=df.variable.apply(
                lambda x: "people_fully_vaccinated_per_hundred" if "fully" in x else "people_vaccinated_per_hundred"
            ),
        )
        # Extract age group parameters
        regex = r"coverage_(?:people|fully)_(\d+)_(\d+)"
        df[["age_group_min", "age_group_max"]] = df.variable.str.extract(regex)
        # Pivot back
        return df.pivot(
            index=["date", "age_group_min", "age_group_max"],
            columns="metric",
            values="value",
        ).reset_index()

    def pipe_age_fix_age_max(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(age_group_max=df["age_group_max"].replace({"115": pd.NA}))

    def pipeline_age(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_age_checks)
            .pipe(self.pipe_age_melt_pivot)
            .replace(to_replace=r"%", value="", regex=True)
            .assign(location=self.location)
            .pipe(self.pipe_age_fix_age_max)[
                [
                    "location",
                    "date",
                    "age_group_min",
                    "age_group_max",
                    "people_fully_vaccinated_per_hundred",
                    "people_vaccinated_per_hundred",
                ]
            ]
            .sort_values(["location", "date", "age_group_min"])
        )

    def to_csv(self):
        # Load data
        df, df_age = self.read()
        # Export main
        df.pipe(self.pipeline).to_csv(paths.out_vax(self.location), index=False)
        # Export manufacturer data
        df_man = df.pipe(self.pipeline_manufacturer)
        df_man.to_csv(paths.out_vax(self.location, manufacturer=True), index=False)
        export_metadata_manufacturer(
            df_man,
            "Ministry of Health via vacuna.uy",
            self.source_url,
        )
        # Export age data
        df_age = df_age.pipe(self.pipeline_age)
        df_age.to_csv(paths.out_vax(self.location, age=True), index=False)
        export_metadata_age(
            df_age,
            "Ministry of Health via vacuna.uy",
            self.source_url_age,
        )


def main():
    Uruguay().to_csv()


if __name__ == "__main__":
    main()
