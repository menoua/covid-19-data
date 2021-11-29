import re

import pandas as pd

from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.utils import paths


class Germany:
    source_url: str = "https://impfdashboard.de/static/data/germany_vaccinations_timeseries_v2.tsv"
    source_url_ref: str = "https://impfdashboard.de/"
    location: str = "Germany"
    columns_rename: str = {
        "dosen_kumulativ": "total_vaccinations",
        "personen_erst_kumulativ": "people_vaccinated",
        "dosen_dritt_kumulativ": "total_boosters",
    }
    vaccine_mapping: str = {
        "dosen_biontech_kumulativ": "Pfizer/BioNTech",
        "dosen_moderna_kumulativ": "Moderna",
        "dosen_astra_kumulativ": "Oxford/AstraZeneca",
        "dosen_johnson_kumulativ": "Johnson&Johnson",
    }
    fully_vaccinated_mapping: str = {
        "dosen_biontech_zweit_kumulativ": "full_biontech",
        "dosen_moderna_zweit_kumulativ": "full_moderna",
        "dosen_johnson_erst_kumulativ": "full_jj",
        "dosen_astra_zweit_kumulativ": "full_astra",
    }
    regex_doses_colnames: str = r"dosen_([a-zA-Z]*)_kumulativ"

    def read(self):
        return pd.read_csv(self.source_url, sep="\t")

    def _check_vaccines(self, df: pd.DataFrame):
        """Get vaccine columns mapped to Vaccine names."""
        EXCLUDE = ["kbv", "dim", "erst", "zweit", "dritt"]

        def _is_vaccine_column(column_name: str):
            if re.search(self.regex_doses_colnames, column_name):
                if re.search(self.regex_doses_colnames, column_name).group(1) not in EXCLUDE:
                    return True
            return False

        for column_name in df.columns:
            if _is_vaccine_column(column_name) and column_name not in self.vaccine_mapping:
                raise ValueError(f"Found unknown vaccine: {column_name}")
        return df

    def translate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.columns_rename)

    def translate_vaccine_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.vaccine_mapping).rename(columns=self.fully_vaccinated_mapping)

    def calculate_fully_vaccinated(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(people_fully_vaccinated=df.full_biontech + df.full_moderna + df.full_jj + df.full_astra)

    def enrich_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location="Germany")

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self._check_vaccines)
            .pipe(self.translate_columns)
            .pipe(self.translate_vaccine_columns)
            .pipe(self.calculate_fully_vaccinated)
            .pipe(self.enrich_location)
        )

    def enrich_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_ref)

    def _vaccine_start_dates(self, df: pd.DataFrame):
        date2vax = sorted(
            ((df.loc[df[vaccine] > 0, "date"].min(), vaccine) for vaccine in self.vaccine_mapping.values()),
            key=lambda x: x[0],
            reverse=True,
        )
        return [(date2vax[i][0], ", ".join(sorted(v[1] for v in date2vax[i:]))) for i in range(len(date2vax))]

    def enrich_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        vax_date_mapping = self._vaccine_start_dates(df)

        def _enrich_vaccine(date: str) -> str:
            for dt, vaccines in vax_date_mapping:
                if date >= dt:
                    return vaccines
            raise ValueError(f"Invalid date {date} in DataFrame!")

        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            [
                "date",
                "location",
                "vaccine",
                "source_url",
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
                "total_boosters",
            ]
        ]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.enrich_source).pipe(self.enrich_vaccine).pipe(self.select_output_columns)

    def melt_manufacturers(self, df: pd.DataFrame) -> pd.DataFrame:
        id_vars = ["date", "location"]
        return df[id_vars + list(self.vaccine_mapping.values())].melt(
            id_vars=id_vars, var_name="vaccine", value_name="total_vaccinations"
        )

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.melt_manufacturers)

    def export(self):
        df_base = self.read().pipe(self.pipeline_base)
        # Export data
        df = df_base.pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)
        # Export manufacturer data
        df = df_base.pipe(self.pipeline_manufacturer)
        df.to_csv(paths.out_vax(self.location, manufacturer=True), index=False)
        export_metadata_manufacturer(df, "Robert Koch Institut", self.source_url_ref)


def main():
    Germany().export()


if __name__ == "__main__":
    main()
