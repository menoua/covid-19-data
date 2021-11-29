import pandas as pd

from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.utils import paths


vaccine_mapping = {
    "Pfizer": "Pfizer/BioNTech",
    "Sinovac": "Sinovac",
    "Astra-Zeneca": "Oxford/AstraZeneca",
    "CanSino": "CanSino",
}


class Chile:
    def __init__(self):
        self.location = "Chile"
        # Alternative: https://github.com/MinCiencia/Datos-COVID19/tree/master/output/producto83
        self.source_url_manufacturer = (
            "https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto76/fabricante.csv"
        )
        self.source_url_vaccinations = (
            "https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto76/vacunacion.csv"
        )
        self.source_url_ref = "https://github.com/MinCiencia/Datos-COVID19"

    # Generalized methods
    def read(self, url: str) -> pd.DataFrame:
        return pd.read_csv(url)

    def pipe_melt(self, df: pd.DataFrame, id_vars: list) -> pd.DataFrame:
        return df.melt(id_vars, var_name="date", value_name="value")

    def pipe_pivot(self, df: pd.DataFrame, index: list) -> pd.DataFrame:
        return df.pivot(index=index, columns="Dosis", values="value").reset_index()

    # Vaccination methods
    def pipe_keep_total(self, df: pd.DataFrame, colname: str) -> pd.DataFrame:
        return df[(df[colname] == "Total") & (df.value > 0)]

    def pipe_calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.fillna(0)
        return df.assign(
            people_vaccinated=df.Primera + df.Unica,
            people_fully_vaccinated=df.Segunda + df.Unica,
            total_vaccinations=df.Primera + df.Refuerzo + df.Segunda + df.Unica,
            total_boosters=df.Refuerzo,
        ).drop(columns=["Primera", "Refuerzo", "Segunda", "Unica"])

    def pipe_add_vaccine_list(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.merge(self.vaccine_list, on="date", how="left").sort_values("date")
        df["vaccine"] = df.vaccine.ffill()
        return df

    def pipe_add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns="Region").assign(location=self.location, source_url=self.source_url_ref)

    def pipeline_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_melt, ["Region", "Dosis"])
            .pipe(self.pipe_keep_total, "Region")
            .pipe(self.pipe_pivot, ["Region", "date"])
            .pipe(self.pipe_calculate_metrics)
            .pipe(self.pipe_add_vaccine_list)
            .pipe(self.pipe_add_metadata)
            .sort_values("date")
        )

    # Manufacturer methods
    def pipe_exclude_total(self, df: pd.DataFrame, colname: str) -> pd.DataFrame:
        return df[(df[colname] != "Total") & (df.value > 0)]

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.rename(columns={"Fabricante": "vaccine", "value": "total_vaccinations"})
            .assign(total_vaccinations=df.Primera.fillna(0) + df.Segunda.fillna(0))
            .drop(columns=["Primera", "Segunda"])
        )

    def pipe_rename_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccines_wrong = set(df["vaccine"].unique()).difference(vaccine_mapping)
        if vaccines_wrong:
            raise ValueError(f"Missing vaccines: {vaccines_wrong}")
        return df.replace(vaccine_mapping)

    def save_vaccine_list(self, df: pd.DataFrame) -> pd.DataFrame:
        self.vaccine_list = (
            df.sort_values("vaccine").groupby("date", as_index=False).agg({"vaccine": lambda x: ", ".join(x)})
        )
        return df

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_melt, ["Fabricante", "Dosis"])
            .pipe(self.pipe_exclude_total, "Fabricante")
            .pipe(self.pipe_pivot, ["Fabricante", "date"])
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_rename_vaccines)
            .pipe(self.save_vaccine_list)
            .assign(location=self.location)[["location", "date", "vaccine", "total_vaccinations"]]
            .sort_values(["location", "date", "vaccine"])
        )

    def to_csv(self):
        # Manufacturer
        df_man = self.read(self.source_url_manufacturer).pipe(self.pipeline_manufacturer)
        df_man.to_csv(paths.out_vax(self.location, manufacturer=True), index=False)
        export_metadata_manufacturer(
            df_man,
            "Ministerio de Ciencia, Tecnología, Conocimiento e Innovación",
            self.source_url_ref,
        )

        # Main data
        df = self.read(self.source_url_vaccinations).pipe(self.pipeline_vaccinations)
        df.to_csv(paths.out_vax(self.location), index=False)


def main():
    Chile().to_csv()


if __name__ == "__main__":
    main()
