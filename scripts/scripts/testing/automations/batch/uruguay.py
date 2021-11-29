import json
import os
from urllib.request import Request, urlopen

import pandas as pd

from cowidev.utils import paths
from cowidev.utils.clean import clean_date_series
from cowidev.utils.web import request_json


class Uruguay:
    location: str = "Uruguay"
    units: str = "people tested"
    source_label: str = "Ministry of Public Health"
    source_url: str = "https://estadisticas.msp-uy.com/data.json"
    notes = ""
    testing_type: str = "PCR only"
    rename_columns: dict = {"date": "Date", "total": "Cumulative total"}

    def read(self) -> pd.DataFrame:
        json_dict = request_json(self.source_url)
        return pd.DataFrame.from_dict(json_dict["tests"]["historical"])

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.rename_columns)

    @staticmethod
    def pipe_date(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(Date=clean_date_series(df.Date, "%Y-%m-%d"))

    @staticmethod
    def pipe_metrics(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(**{"Cumulative total": df["Cumulative total"].cumsum()})

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            **{
                "Country": self.location,
                "Units": self.units,
                "Source label": self.source_label,
                "Source URL": self.source_url,
                "Notes": self.notes,
            }
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_rename_columns).pipe(self.pipe_date).pipe(self.pipe_metrics).pipe(self.pipe_metadata)

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(os.path.join(paths.SCRIPTS.OLD, "testing", "automated_sheets", f"{self.location}.csv"), index=False)


def main():
    Uruguay().export()


if __name__ == "__main__":
    main()
