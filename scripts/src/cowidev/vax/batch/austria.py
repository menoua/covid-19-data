import pandas as pd
from cowidev.utils import paths


class Austria:
    location: str = "Austria"
    source_url: str = "https://info.gesundheitsministerium.gv.at/data/COVID19_vaccination_doses_timeline.csv"
    source_url_ref: str = "https://info.gesundheitsministerium.gv.at/opendata/"
    vaccine_mapping: dict = {
        "BioNTechPfizer": "Pfizer/BioNTech",
        "Moderna": "Moderna",
        "AstraZeneca": "Oxford/AstraZeneca",
        "Janssen": "Johnson&Johnson",
    }
    one_dose_vaccines: str = ["Janssen"]

    def read(self) -> pd.DataFrame:
        return pd.read_csv(
            self.source_url,
            sep=";",
            usecols=["date", "state_name", "vaccine", "dose_number", "doses_administered_cumulative"],
        )

    def pipe_filter_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[(df["state_name"] == "Österreich") & (df.vaccine != "Other")].drop(columns="state_name")

    def pipe_check_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccine_names = set(df.vaccine)
        unknown_vaccines = set(vaccine_names).difference(self.vaccine_mapping.keys())
        if unknown_vaccines:
            raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=df.date.str.slice(0, 10))

    def pipe_reshape(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pivot(
            index=["date", "vaccine"], columns="dose_number", values="doses_administered_cumulative"
        ).reset_index()

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        assert [*df.columns] == ["date", "vaccine", 1, 2, 3], "Wrong list of columns! Maybe a 4th dose was added?"

        # Total vaccinations
        df.loc[:, "total_vaccinations"] = df[1] + df[2] + df[3]

        # People vaccinated
        df.loc[:, "people_vaccinated"] = df[1]

        # People fully vaccinated
        df.loc[df.vaccine.isin(self.one_dose_vaccines), "people_fully_vaccinated"] = df[1]
        df.loc[-df.vaccine.isin(self.one_dose_vaccines), "people_fully_vaccinated"] = df[2]

        # Total boosters
        df.loc[df.vaccine.isin(self.one_dose_vaccines), "total_boosters"] = df[2] + df[3]
        df.loc[-df.vaccine.isin(self.one_dose_vaccines), "total_boosters"] = df[3]

        return (
            df[
                [
                    "date",
                    "people_vaccinated",
                    "people_fully_vaccinated",
                    "total_vaccinations",
                    "total_boosters",
                ]
            ]
            .groupby("date", as_index=False)
            .sum()
        )

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url,
        )

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _make_list(date: str) -> str:
            vax_list = ["Pfizer/BioNTech"]
            if date >= "2021-01-15":
                vax_list.append("Moderna")
            if date >= "2021-02-08":
                vax_list.append("Oxford/AstraZeneca")
            if date >= "2021-03-15":
                vax_list.append("Johnson&Johnson")
            return ", ".join(sorted(vax_list))

        df["vaccine"] = df.date.apply(_make_list)
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_filter_rows)
            .pipe(self.pipe_check_vaccine)
            .pipe(self.pipe_date)
            .pipe(self.pipe_reshape)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_vaccine)
            .sort_values("date")
        )

    def export(self):
        destination = paths.out_vax(self.location)
        self.read().pipe(self.pipeline).to_csv(destination, index=False)


def main():
    Austria().export()
