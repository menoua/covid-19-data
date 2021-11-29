import pandas as pd

from cowidev.utils.clean import clean_df_columns_multiindex, clean_date_series
from cowidev.utils.web.download import read_xlsx_from_url
from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.utils import paths


class SouthKorea:
    def __init__(self):
        self.location = "South Korea"
        self.source_url = "https://ncv.kdca.go.kr/filepath/boardDownload.es?bid=9999&list_no=9999&seq=1"
        self.source_url_ref = "https://ncv.kdca.go.kr/"
        self.vaccines_mapping = {
            "모더나 누적": "Moderna",
            "아스트라제네카 누적": "Oxford/AstraZeneca",
            "화이자 누적": "Pfizer/BioNTech",
            "얀센 누적": "Johnson&Johnson",
        }

    def read(self):
        df = read_xlsx_from_url(self.source_url, header=[4, 5, 6])
        df = self._drop_invalid_columns(df)
        df = clean_df_columns_multiindex(df)
        return df

    def _drop_invalid_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Drop weird columns. May break if new columns are added with mostly NaN values
        nulls = df.isnull().sum()
        n_rows = len(df)
        columns_discard = [nulls.index[i] for i, n in enumerate(nulls) if n > 0.95 * n_rows]
        return df.drop(columns=columns_discard)

    def pipe_check_format(self, df: pd.DataFrame) -> pd.DataFrame:
        # if df.shape[1] != 10:
        #     raise ValueError("Number of columns has changed!")
        columns_lv = dict()
        columns_lv[0] = {"기본 접종", "일자", "전체 누적", "추가 접종"}
        columns_lv[1] = {"모더나 누적", "아스트라제네카 누적", "얀센 누적", "화이자 누적"}
        columns_lv[2] = {
            "",
            "1차",
            "1차(완료)",
            "완료",
            "완료\n(AZ-PF교차미포함)",
            "완료\n(M-Pf 교차 포함)",
            "완료\n(AZ-PF교차포함)",
            "완료\n(교차미포함)",
            "추가",
        }

        columns_lv_wrong = {i: df.columns.levels[i].difference(k) for i, k in columns_lv.items()}

        for lv, diff in columns_lv_wrong.items():
            if any(diff):
                raise ValueError(f"Unknown columns in level {lv}: {diff}")
        return df

    def pipe_extract(self, df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": df.loc[:, "일자"],
                "people_vaccinated": df.loc[:, ("전체 누적", "", "1차")],
                "people_fully_vaccinated": df.loc[:, ("전체 누적", "", "완료")],
                "total_boosters": df.loc[:, ("전체 누적", "", "추가")],
                "janssen": df.loc[:, ("기본 접종", "얀센 누적", "1차(완료)")],
            }
        )

    def pipe_extract_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        data = {"date": df.loc[:, "일자"]}
        for vax_og, vax_new in self.vaccines_mapping.items():
            primary = df.loc[:, ("기본 접종", vax_og)].sum(axis=1)
            booster = df.loc[:, "추가 접종"].get(vax_og)
            data[vax_new] = primary + (0 if booster is None else booster)
        return pd.DataFrame(data)

    def pipe_melt_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(df.melt(id_vars="date", var_name="vaccine", value_name="total_vaccinations"))

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_ref)

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df["people_vaccinated"]
            + df["people_fully_vaccinated"]
            - df["janssen"]
            + df["total_boosters"]
        )

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date))

    def pipe_vac(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str):
            if date >= "2021-06-18":
                return "Oxford/AstraZeneca, Pfizer/BioNTech, Johnson&Johnson, Moderna"
            elif date >= "2021-06-10":
                return "Oxford/AstraZeneca, Pfizer/BioNTech, Johnson&Johnson"
            elif date >= "2021-02-27":
                return "Oxford/AstraZeneca, Pfizer/BioNTech"
            elif date >= "2021-02-26":
                return "Oxford/AstraZeneca"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_check_format)
            .pipe(self.pipe_extract)
            .pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_date)
            .pipe(self.pipe_source)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vac)[
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
            .sort_values("date")
            .drop_duplicates()
        )

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_check_format)
            .pipe(self.pipe_extract_manufacturer)
            .pipe(self.pipe_date)
            .pipe(self.pipe_melt_manufacturer)
            .pipe(self.pipe_location)
            .sort_values(["date", "vaccine"])
            .drop_duplicates()
            .reset_index(drop=True)
        )

    def export(self):
        df = self.read()
        # Main data
        df.pipe(self.pipeline).to_csv(paths.out_vax(self.location), index=False)
        # Vaccination by manufacturer
        df_man = df.pipe(self.pipeline_manufacturer)
        df_man.to_csv(paths.out_vax(self.location, manufacturer=True), index=False)
        export_metadata_manufacturer(
            df_man,
            "Korea Centers for Disease Control and Prevention",
            self.source_url_ref,
        )


def main():
    SouthKorea().export()
