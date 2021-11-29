from datetime import datetime

import pandas as pd

from cowidev.vax.utils.files import export_metadata_age, export_metadata_manufacturer
from cowidev.utils.web import request_json, get_soup
from cowidev.utils.clean import clean_date_series
from cowidev.vax.utils.checks import validate_vaccines
from cowidev.utils import paths


class Switzerland:
    def __init__(self):
        self.source_url = "https://opendata.swiss/en/dataset/covid-19-schweiz"

    def read(self):
        doses_url, people_url, manufacturer_url = self._get_file_url()
        df, df_manufacturer = self._parse_data(doses_url, people_url, manufacturer_url)
        df_age = self.read_age()
        return df, df_manufacturer, df_age

    def read_age(self):
        soup = get_soup(self.source_url)
        url = self._parse_age_link(soup)
        return pd.read_csv(url)

    def _parse_age_link(self, soup):
        elems = soup.find_all(class_="resource-item")
        elem = list(
            filter(lambda x: (x.a.get("title") == "COVID19VaccPersons_AKL10_w_v2") & (x.small.text == "CSV"), elems)
        )[0]
        return elem.find(class_="btn").get("href")

    def _get_file_url(self) -> str:
        response = request_json("https://www.covid19.admin.ch/api/data/context")
        context = response["sources"]["individual"]["csv"]
        doses_url = context["vaccDosesAdministered"]
        people_url = context["vaccPersonsV2"]
        manufacturer_url = context["weeklyVacc"]["byVaccine"]["vaccDosesAdministered"]
        return doses_url, people_url, manufacturer_url

    def _parse_data(self, doses_url, people_url, manufacturer_url):
        # print(doses_url)
        # print(people_url)
        # print(manufacturer_url)
        doses = pd.read_csv(
            doses_url,
            usecols=["geoRegion", "date", "sumTotal", "type"],
        )
        people = pd.read_csv(
            people_url,
            usecols=["geoRegion", "date", "sumTotal", "type", "age_group"],
        )
        people = people[people.age_group == "total_population"].drop(columns=["age_group"])
        manufacturer = pd.read_csv(
            manufacturer_url,
            usecols=["date", "geoRegion", "vaccine", "sumTotal"],
        )
        return pd.concat([doses, people], ignore_index=True), manufacturer

    def pipe_filter_country(self, df: pd.DataFrame, country_code: str) -> pd.DataFrame:
        return df[df.geoRegion == country_code].drop(columns=["geoRegion"])

    def pipe_unique_rows(self, df: pd.DataFrame):
        # Checks
        a = df.groupby(["date", "type"]).count().reset_index()
        if not a[a.sumTotal > 1].empty:
            raise ValueError("Duplicated rows in either `people` or `doses` dataframes!")
        return df

    def pipe_pivot(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pivot(index=["date"], columns="type", values="sumTotal").reset_index().sort_values("date")

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "COVID19FullyVaccPersons": "people_fully_vaccinated",
                "COVID19VaccDosesAdministered": "total_vaccinations",
                "COVID19AtLeastOneDosePersons": "people_vaccinated",
            }
        )

    def pipe_fix_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[df.total_vaccinations < df.people_vaccinated, "total_vaccinations"] = df.people_vaccinated
        return df

    def pipe_location(self, df: pd.DataFrame, location: str) -> pd.DataFrame:
        return df.assign(location=location)

    def pipe_source(self, df: pd.DataFrame, country_code: str) -> pd.DataFrame:
        return df.assign(
            source_url=f"{self.source_url}?detGeo={country_code}",
        )

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            if date >= "2021-01-29":
                return "Moderna, Pfizer/BioNTech"
            return "Pfizer/BioNTech"

        return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))

    def pipeline(self, df: pd.DataFrame, location: str) -> pd.DataFrame:
        geo_region = _get_geo_region(location)
        return (
            df.pipe(self.pipe_filter_country, geo_region)
            .pipe(self.pipe_unique_rows)
            .pipe(self.pipe_pivot)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_fix_metrics)
            .pipe(self.pipe_location, location)
            .pipe(self.pipe_source, geo_region)
            .pipe(self.pipe_vaccine)[
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

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccine_mapping = {
            "pfizer_biontech": "Pfizer/BioNTech",
            "moderna": "Moderna",
            "johnson_johnson": "Johnson&Johnson",
        }
        validate_vaccines(df, vaccine_mapping)
        return (
            df.rename(columns={"sumTotal": "total_vaccinations"})[df.geoRegion == "CH"]
            .drop(columns="geoRegion")
            .assign(location="Switzerland")
            .replace(vaccine_mapping)
        )

    def pipe_age_filter_region(self, df, geo_region):
        # Only Switzerland
        return df[(df.geoRegion == geo_region) & (df.age_group_type == "age_group_AKL10")]

    def pipe_age_checks(self, df):
        # Check population per age group is unique
        if not (df.groupby("altersklasse_covid19").pop.nunique() == 1).all():
            raise ValueError("Different `pop` values for same `alterklasse_covid19` value!")
        # Check type
        type_wrong = set(df.type_variant).difference(["altersklasse_covid19"])
        if type_wrong:
            raise ValueError(f"Invalid `type_variant` value: {type_wrong}")
        # Date+Age group uniqueness
        if not (df.groupby(["date", "altersklasse_covid19"]).type.value_counts() == 1).all():
            raise ValueError("Some dates and age groups have multiple entries for same metric!")
        return df

    def pipe_age_pivot(self, df):
        return df.pivot(
            index=["date", "altersklasse_covid19"], columns=["type"], values="per100PersonsTotal"
        ).reset_index()

    def pipe_age_date(self, df):
        return df.assign(date=clean_date_series(df.date.apply(lambda x: datetime.strptime(str(x) + "+0", "%G%V+%w"))))

    def pipe_age_location(self, df, location):
        return df.assign(location=location)

    def pipe_age_rename_columns(self, df):
        return df.rename(
            columns={
                "altersklasse_covid19": "age_group",
                "COVID19AtLeastOneDosePersons": "people_vaccinated_per_hundred",
                "COVID19FullyVaccPersons": "people_fully_vaccinated_per_hundred",
            }
        )

    def pipe_age_groups(self, df):
        regex = r"(\d{1,2})+?(?: - (\d{1,2}))?"
        df[["age_group_min", "age_group_max"]] = df.age_group.str.extract(regex)
        return df

    def pipe_age_select_cols(self, df):
        return df[
            [
                "location",
                "date",
                "age_group_min",
                "age_group_max",
                "people_vaccinated_per_hundred",
                "people_fully_vaccinated_per_hundred",
            ]
        ]

    def pipeline_age(self, df, location):
        geo_region = _get_geo_region(location)

        df_ = df.copy()
        return (
            df_.pipe(self.pipe_age_filter_region, geo_region)
            .pipe(self.pipe_age_checks)
            .pipe(self.pipe_age_pivot)
            .pipe(self.pipe_age_date)
            .pipe(self.pipe_age_location, location)
            .pipe(self.pipe_age_rename_columns)
            .pipe(self.pipe_age_groups)
            .pipe(self.pipe_age_select_cols)
        )

    def export(self):
        locations = ["Switzerland", "Liechtenstein"]
        df, df_manuf, df_age = self.read()

        # Main data
        for location in locations:
            df.pipe(self.pipeline, location).to_csv(paths.out_vax(location), index=False)

        # Manufacturer
        df_manuf = df_manuf.pipe(self.pipeline_manufacturer)
        df_manuf.to_csv(paths.out_vax("Switzerland", manufacturer=True), index=False)
        export_metadata_manufacturer(
            df_manuf,
            "Federal Office of Public Health",
            self.source_url,
        )

        # Age
        for location in locations:
            df_age_ = df_age.pipe(self.pipeline_age, location)
            df_age_.to_csv(paths.out_vax(location, age=True), index=False)
            export_metadata_age(
                df_age_,
                "Federal Office of Public Health",
                self.source_url,
            )


def main():
    Switzerland().export()


def _get_geo_region(location):
    if location == "Switzerland":
        return "CH"
    elif location == "Liechtenstein":
        return "FL"
    else:
        raise ValueError("Only Switzerland or Liechtenstein are accepted values for `location`.")
