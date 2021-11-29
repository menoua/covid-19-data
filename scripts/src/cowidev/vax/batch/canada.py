import pandas as pd

from cowidev.utils.web import request_json
from cowidev.utils import paths


class Canada:
    location: str = "Canada"
    source_url: str = "https://api.covid19tracker.ca/reports"
    source_url_ref: str = "https://covid19tracker.ca/vaccinationtracker.html"
    source_url_boosters: str = "https://api.covid19tracker.ca/vaccines/reports/latest"
    df_boosters: pd.DataFrame = None

    def read(self) -> pd.DataFrame:
        data = request_json(self.source_url)
        return pd.DataFrame.from_records(
            data["data"], columns=["date", "total_vaccinations", "total_vaccinated", "total_boosters_1"]
        )

    # def _get_boosters_data(self):
    #     data = request_json(self.source_url_boosters)
    #     df = pd.DataFrame.from_records(data["data"])
    #     total_boosters = df.total_boosters_1.sum().astype(int)
    #     total_boosters_date = df.date.max()
    #     return total_boosters_date, total_boosters

    def pipe_filter_rows(self, df: pd.DataFrame):
        # Only records since vaccination campaign started
        return df[df.total_vaccinations > 0]

    def pipe_rename_columns(self, df: pd.DataFrame):
        return df.rename(columns={"total_vaccinated": "people_fully_vaccinated", "total_boosters_1": "total_boosters"})

    def pipe_people_vaccinated(self, df: pd.DataFrame):
        df = df.assign(people_vaccinated=(df.total_vaccinations - df.people_fully_vaccinated - df.total_boosters))
        # # Booster data was not recorded for this dates, hence estimations on people vaccinated will not be accurate
        # df.loc[(df.date >= "2021-10-04") & (df.date <= "2021-10-09"), "people_vaccinated"] = pd.NA
        return df

    def pipe_metadata(self, df: pd.DataFrame):
        return df.assign(
            location=self.location,
            source_url=self.source_url_ref,
            vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
        )

    # def pipe_total_boosters(self, df: pd.DataFrame):
    #     df = df.merge(self.df_boosters, how="left", on="date")
    #     dt, v = self._get_boosters_data()
    #     df.loc[df.date == dt, "total_boosters"] = v
    #     return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.pipe(self.pipe_filter_rows)
            .pipe(self.pipe_rename_columns)
            # .pipe(self.pipe_total_boosters)
            .pipe(self.pipe_people_vaccinated)
            .pipe(self.pipe_metadata)
            .sort_values("date")
        )
        return df

    def export(self):
        destination = paths.out_vax(self.location)
        # self.df_boosters = self._load_current_df()
        self.read().pipe(self.pipeline).to_csv(destination, index=False)

    # def _load_current_df(self):
    #     df = pd.read_csv(paths.out_vax(self.location))
    #     return df[["date", "total_boosters"]]


def main():
    Canada().export()
