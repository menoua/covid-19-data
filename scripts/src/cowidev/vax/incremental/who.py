import pandas as pd
import numpy as np

from cowidev.vax.utils.incremental import increment
from cowidev.vax.utils.checks import VACCINES_ONE_DOSE
from cowidev.vax.utils.orgs import WHO_VACCINES, WHO_COUNTRIES
from cowidev.vax.cmd.utils import get_logger


logger = get_logger()


# Sometimes the WHO doesn't yet include a vaccine in a country's metadata
# while there is evidence that it has been administered in the country
ADDITIONAL_VACCINES_USED = {
    "Cayman Islands": ["Oxford/AstraZeneca"],
    "Gambia": ["Johnson&Johnson"],
}


class WHO:
    def __init__(self) -> None:
        self.source_url = "https://covid19.who.int/who-data/vaccination-data.csv"
        self.source_url_ref = "https://covid19.who.int/"

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.source_url)

    def pipe_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        if len(df) > 300:
            raise ValueError(f"Check source, it may contain updates from several dates! Shape found was {df.shape}")
        if df.groupby("COUNTRY").DATE_UPDATED.nunique().nunique() == 1:
            if df.groupby("COUNTRY").DATE_UPDATED.nunique().unique()[0] != 1:
                raise ValueError("Countries have more than one date update!")
        else:
            raise ValueError("Countries have more than one date update!")
        return df

    def pipe_rename_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        df["COUNTRY"] = df.COUNTRY.replace(WHO_COUNTRIES)
        return df

    def pipe_filter_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Get valid entries:

        - Countries not coming from OWID (avoid loop)
        - Rows with total_vaccinations >= people_vaccinated >= people_fully_vaccinated
        """
        df = df[df.DATA_SOURCE == "REPORTING"].copy()
        mask_1 = (
            df.TOTAL_VACCINATIONS >= df.PERSONS_VACCINATED_1PLUS_DOSE
        ) | df.PERSONS_VACCINATED_1PLUS_DOSE.isnull()
        mask_2 = (df.TOTAL_VACCINATIONS >= df.PERSONS_FULLY_VACCINATED) | df.PERSONS_FULLY_VACCINATED.isnull()
        mask_3 = (
            (df.PERSONS_VACCINATED_1PLUS_DOSE >= df.PERSONS_FULLY_VACCINATED)
            | df.PERSONS_VACCINATED_1PLUS_DOSE.isnull()
            | df.PERSONS_FULLY_VACCINATED.isnull()
        )
        df = df[(mask_1 & mask_2 & mask_3)]
        df = df[df.COUNTRY.isin(WHO_COUNTRIES.values())]
        return df

    def pipe_vaccine_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccines_used = set(df.VACCINES_USED.dropna().apply(lambda x: [xx.strip() for xx in x.split(",")]).sum())
        vaccines_unknown = vaccines_used.difference(WHO_VACCINES)
        if vaccines_unknown:
            raise ValueError(f"Unknown vaccines {vaccines_unknown}. Update `vax.utils.who.config` accordingly.")
        return df

    def _map_vaccines_func(self, row) -> tuple:
        """Replace vaccine names and create column `only_2_doses`."""
        if pd.isna(row.VACCINES_USED):
            raise ValueError("Vaccine field is NaN")
        vaccines = pd.Series(row.VACCINES_USED.split(","))
        vaccines = vaccines.replace(WHO_VACCINES)
        only_2doses = all(-vaccines.isin(pd.Series(VACCINES_ONE_DOSE)))

        # Add vaccines that aren't yet recorded by the WHO
        if row.COUNTRY in ADDITIONAL_VACCINES_USED.keys():
            vaccines = pd.concat([vaccines, pd.Series(ADDITIONAL_VACCINES_USED[row.COUNTRY])])

        return pd.Series([", ".join(sorted(vaccines.unique())), only_2doses])

    def pipe_map_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Based on the list of known vaccines, identifies whether each country is using only 2-dose
        vaccines or also some 1-dose vaccines. This determines whether people_fully_vaccinated can be
        calculated as total_vaccinations - people_vaccinated.
        Vaccines check
        """
        df[["VACCINES_USED", "only_2doses"]] = df.apply(self._map_vaccines_func, axis=1)
        return df

    def pipe_calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df[["PERSONS_VACCINATED_1PLUS_DOSE", "PERSONS_FULLY_VACCINATED"]] = (
            df[["PERSONS_VACCINATED_1PLUS_DOSE", "PERSONS_FULLY_VACCINATED"]].astype("Int64").fillna(pd.NA)
        )
        df.loc[:, "TOTAL_VACCINATIONS"] = df["TOTAL_VACCINATIONS"].fillna(np.nan)
        return df

    def increment_countries(self, df: pd.DataFrame):
        for row in df.sort_values("COUNTRY").iterrows():
            row = row[1]
            cond = (
                row[
                    [
                        "PERSONS_VACCINATED_1PLUS_DOSE",
                        "PERSONS_FULLY_VACCINATED",
                        "TOTAL_VACCINATIONS",
                    ]
                ]
                .isnull()
                .all()
            )
            if not cond:
                increment(
                    location=row["COUNTRY"],
                    total_vaccinations=row["TOTAL_VACCINATIONS"],
                    people_vaccinated=row["PERSONS_VACCINATED_1PLUS_DOSE"],
                    people_fully_vaccinated=row["PERSONS_FULLY_VACCINATED"],
                    date=row["DATE_UPDATED"],
                    vaccine=row["VACCINES_USED"],
                    source_url=self.source_url_ref,
                )
                country = row["COUNTRY"]
                logger.info(f"\tcowidev.vax.incremental.who.{country}: SUCCESS ✅")

    def pipeline(self, df: pd.DataFrame):
        return (
            df.pipe(self.pipe_checks)
            .pipe(self.pipe_rename_countries)
            .pipe(self.pipe_filter_entries)
            .pipe(self.pipe_vaccine_checks)
            .pipe(self.pipe_map_vaccines)
            .pipe(self.pipe_calculate_metrics)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.increment_countries(df)


def main():
    WHO().export()
