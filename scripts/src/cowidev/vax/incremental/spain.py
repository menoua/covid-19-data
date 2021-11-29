import os
import re
from datetime import datetime, timedelta
from urllib.error import HTTPError
from cowidev.utils.clean.dates import clean_date_series

import pandas as pd

from cowidev.utils.clean import clean_date
from cowidev.vax.utils.incremental import merge_with_current_data
from cowidev.utils import paths


class Spain:
    location: str = "Spain"
    vaccine_mapping: dict = {
        "Pfizer": "Pfizer/BioNTech",
        "Moderna": "Moderna",
        "AstraZeneca": "Oxford/AstraZeneca",
        "Janssen": "Johnson&Johnson",
    }
    _date_field_raw: str = "Fecha de la última vacuna registrada (2)"
    _max_days_back: int = 20

    def read(self, last_update: str) -> pd.Series:
        return self._parse_data(last_update)

    def _parse_data(self, last_update: str):
        records = []
        for days in range(self._max_days_back):
            date_it = clean_date(datetime.now() - timedelta(days=days))
            # print(date_it)
            # print(f"{date_it} > {last_update}?")
            if date_it > last_update:
                source = self._get_source_url(date_it.replace("-", ""))
                try:
                    df_ = pd.read_excel(source, index_col=0, parse_dates=[self._date_field_raw])
                except HTTPError:
                    print("No available!")
                else:
                    # print("Adding!")
                    self._check_vaccine_names(df_)
                    ds = self._parse_ds_data(df_, source)
                    records.append(ds)
            else:
                # print("End!")
                break
        if len(records) > 0:
            return pd.DataFrame(records)
        print("No data being added to Spain")
        return None

    def _parse_ds_data(self, df: pd.DataFrame, source: str) -> pd.Series:
        df.loc[~df.index.isin(["Sanidad Exterior"]), self._date_field_raw].dropna().max()
        data = {
            "total_vaccinations": df.loc["Totales", "Dosis administradas (2)"].item(),
            "people_vaccinated": df.loc["Totales", "Nº Personas con al menos 1 dosis"].item(),
            "people_fully_vaccinated": df.loc["Totales", "Nº Personas vacunadas(pauta completada)"].item(),
            "date": clean_date(
                df.loc[
                    ~df.index.isin(["Sanidad Exterior"]),
                    "Fecha de la última vacuna registrada (2)",
                ]
                .dropna()
                .max()
            ),
            "source_url": source,
            "vaccine": ", ".join(self._get_vaccine_names(df, translate=True)),
        }
        col_boosters = "Nº Personas con dosis adicional"
        if col_boosters in df.columns:
            # print("EEE")
            data["total_boosters"] = df.loc["Totales", col_boosters].item()
        return pd.Series(data=data)

    def _get_source_url(self, dt_str):
        return (
            "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/documentos/"
            f"Informe_Comunicacion_{dt_str}.ods"
        )

    def _get_vaccine_names(self, df: pd.DataFrame, translate: bool = False):
        regex_vaccines = r"Dosis entregadas ([a-zA-Z]*) \(1\)"
        if translate:
            return sorted(
                [
                    self.vaccine_mapping[re.search(regex_vaccines, col).group(1)]
                    for col in df.columns
                    if re.match(regex_vaccines, col)
                ]
            )
        else:
            return sorted(
                [re.search(regex_vaccines, col).group(1) for col in df.columns if re.match(regex_vaccines, col)]
            )

    def _check_vaccine_names(self, df: pd.DataFrame):
        vaccines = self._get_vaccine_names(df)
        unknown_vaccines = set(vaccines).difference(self.vaccine_mapping.keys())
        if unknown_vaccines:
            raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df[self._date_field_raw]))

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_location)

    def export(self):
        output_file = paths.out_vax(self.location)
        last_update = pd.read_csv(output_file).date.astype(str).max()
        df = self.read(last_update)
        if df is not None:
            df = df.pipe(self.pipeline)
            df = merge_with_current_data(df, output_file)
            df.to_csv(output_file, index=False)


def main():
    Spain().export()
