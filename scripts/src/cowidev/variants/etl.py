import os
from datetime import timedelta, datetime

import pandas as pd

from cowidev.utils.utils import get_project_dir
from cowidev.utils.clean.dates import clean_date, DATE_FORMAT
from cowidev.utils.web import request_json


class VariantsETL:
    def __init__(self) -> None:
        self.source_url = (
            "https://raw.githubusercontent.com/hodcroftlab/covariants/master/web/data/perCountryData.json"
        )
        self.source_url_date = "https://github.com/hodcroftlab/covariants/raw/master/web/data/update.json"
        # CoVariants -> OWID name mapping. If who=False, variant is placed in bucket "others"
        self.variants_details = {
            "20A.EU2": {"rename": "B.1.160", "who": False},
            "20A/S:439K": {"rename": "B.1.258", "who": False},
            "20A/S:98F": {"rename": "B.1.221", "who": False},
            "20B/S:1122L": {"rename": "B.1.1.302", "who": False},
            "20A/S:126A": {"rename": "B.1.620", "who": False},
            "20B/S:626S": {"rename": "B.1.1.277", "who": False},
            "20B/S:732A": {"rename": "B.1.1.519", "who": False},
            "20C/S:80Y": {"rename": "B.1.367", "who": False},
            "20E (EU1)": {"rename": "B.1.177", "who": False},
            "20H (Beta, V2)": {"rename": "Beta", "who": True},
            "20I (Alpha, V1)": {"rename": "Alpha", "who": True},
            "20J (Gamma, V3)": {"rename": "Gamma", "who": True},
            "21A (Delta)": {"rename": "Delta", "who": True},
            "21B (Kappa)": {"rename": "Kappa", "who": True},
            "21C (Epsilon)": {"rename": "Epsilon", "who": True},
            "21D (Eta)": {"rename": "Eta", "who": True},
            "21F (Iota)": {"rename": "Iota", "who": True},
            "21G (Lambda)": {"rename": "Lambda", "who": True},
            "21H (Mu)": {"rename": "Mu", "who": True},
            "21I (Delta)": {"rename": "Delta", "who": True},
            "21J (Delta)": {"rename": "Delta", "who": True},
            "21K (Omicron)": {"rename": "Omicron", "who": True},
            "S:677H.Robin1": {"rename": "S:677H.Robin1", "who": False},
            "S:677P.Pelican": {"rename": "S:677P.Pelican", "who": False},
        }
        self.country_mapping = {
            "USA": "United States",
            "Czech Republic": "Czechia",
            "Sint Maarten": "Sint Maarten (Dutch part)",
        }
        self.column_rename = {
            "total_sequences": "num_sequences_total",
        }
        self.columns_out = [
            "location",
            "date",
            "variant",
            "num_sequences",
            "perc_sequences",
            "num_sequences_total",
        ]
        self.num_sequences_total_threshold = 30

    @property
    def variants_mapping(self):
        return {k: v["rename"] for k, v in self.variants_details.items()}

    @property
    def variants_who(self):
        return list(set(v["rename"] for v in self.variants_details.values() if v["who"]))

    def extract(self) -> dict:
        data = request_json(self.source_url)
        data = list(filter(lambda x: x["region"] == "World", data["regions"]))[0]["distributions"]
        return data

    @property
    def _parse_last_update_date(self):
        field_name = "lastUpdated"
        date_json = request_json(self.source_url_date)
        if field_name in date_json:
            date_raw = date_json[field_name]
            return datetime.fromisoformat(date_raw).date()
        raise ValueError(f"{field_name} field not found!")

    def transform(self, data: dict) -> pd.DataFrame:
        df = (
            self.json_to_df(data)
            .pipe(self.pipe_filter_by_num_sequences)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_variants)
            .pipe(self.pipe_group_by_variants)
            .pipe(self.pipe_check_variants)
            .pipe(self.pipe_location)
            .pipe(self.pipe_date)
            .pipe(self.pipe_filter_locations)
            .pipe(self.pipe_variant_others)
            .pipe(self.pipe_variant_non_who)
            .pipe(self.pipe_dtypes)
            .pipe(self.pipe_percent)
            .pipe(self.pipe_correct_excess_percentage)
            .pipe(self.pipe_out)
        )
        return df

    def load(self, df: pd.DataFrame, output_path: str) -> None:
        # Export data
        df.to_csv(output_path, index=False)

    def json_to_df(self, data: dict) -> pd.DataFrame:
        df = pd.json_normalize(data, record_path=["distribution"], meta=["country"]).melt(
            id_vars=["country", "total_sequences", "week"],
            var_name="cluster",
            value_name="num_sequences",
        )
        return df

    def pipe_filter_by_num_sequences(self, df: pd.DataFrame) -> pd.DataFrame:
        msk = df.total_sequences >= self.num_sequences_total_threshold
        # Info
        _sk_perc_rows = round(100 * (1 - msk.sum() / len(df)), 2)
        _sk_num_countries = df.loc[-msk, "country"].nunique()
        _sk_countries_top = df[-msk]["country"].value_counts().head(10).to_dict()
        print(
            f"Skipping {msk.sum()} datapoints ({_sk_perc_rows}%), affecting {_sk_num_countries} countries. Some are:"
            f" {_sk_countries_top}"
        )
        return df[msk]

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.column_rename)

    def pipe_variants(self, df: pd.DataFrame) -> pd.DataFrame:
        # Modify/add columns
        df = df.assign(
            variant=df.cluster.str.replace("cluster_counts.", "", regex=True).replace(self.variants_mapping),
        ).drop(columns="cluster")
        return df

    def pipe_group_by_variants(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_values = ["num_sequences"]
        cols_index = [c for c in df.columns if c not in cols_values]
        df = df.groupby(cols_index, as_index=False).sum()
        return df

    def pipe_check_variants(self, df: pd.DataFrame) -> pd.DataFrame:
        variants_missing = set(df.variant).difference(self.variants_mapping.values())
        if variants_missing:
            raise ValueError(f"Unknown variants {variants_missing}. Edit class attribute self.variants_details")
        return df

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(
            location=df.country.replace(self.country_mapping),
        )
        return df.drop(columns=["country"])

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        dt = pd.to_datetime(df.week, format=DATE_FORMAT)
        dt = dt + timedelta(days=14)
        last_update = self._parse_last_update_date
        dt = dt.apply(lambda x: clean_date(min(x.date(), last_update), DATE_FORMAT))
        df = df.assign(
            date=dt,
        )
        return df.drop(columns=["week"])

    def pipe_filter_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        # Filter locations
        populations_path = os.path.join(get_project_dir(), "scripts", "input", "un", "population_latest.csv")
        dfc = pd.read_csv(populations_path)
        df = df[df.location.isin(dfc.entity.unique())]
        return df

    def pipe_variant_others(self, df: pd.DataFrame) -> pd.DataFrame:
        df_a = df[["date", "location", "num_sequences_total"]].drop_duplicates()
        df_b = (
            df.groupby(["date", "location"], as_index=False)
            .agg({"num_sequences": sum})
            .rename(columns={"num_sequences": "all_seq"})
        )
        df_c = df_a.merge(df_b, on=["date", "location"])
        df_c = df_c.assign(others=df_c["num_sequences_total"] - df_c["all_seq"])
        df_c = df_c.melt(
            id_vars=["location", "date", "num_sequences_total"],
            value_vars="num_sequences_others",
            var_name="variant",
            value_name="num_sequences",
        )
        df = pd.concat([df, df_c])
        return df

    def pipe_variant_non_who(self, df: pd.DataFrame) -> pd.DataFrame:
        x = df[-df.variant.isin(self.variants_who)]
        if x.groupby(["location", "date"]).num_sequences_total.nunique().max() != 1:
            raise ValueError("Different value of `num_sequences_total` found for the same location and date")
        x = (
            x.groupby(["location", "date", "num_sequences_total"], as_index=False)
            .agg(
                {
                    "num_sequences": sum,
                }
            )
            .assign(variant="non_who")
        )
        df = pd.concat([df, x], ignore_index=True)
        return df

    def pipe_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.astype({"num_sequences_total": "Int64", "num_sequences": "Int64"})
        return df

    def pipe_percent(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            # perc_sequences=(100 * df["num_sequences"] / df["num_sequences_total"]).round(2),
            perc_sequences=((100 * df["num_sequences"] / df["num_sequences_total"]).round(2))
        )

    def pipe_correct_excess_percentage(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1) `non_who`
        # Get excess
        x = df[df.variant.isin(self.variants_who + ["non_who"])]
        x = x.groupby(["location", "date"], as_index=False).agg({"perc_sequences": sum})
        x = x[abs(x["perc_sequences"] - 100) != 0]
        x["excess"] = x.perc_sequences - 100
        # Merge excess quantity with input df
        df = df.merge(x[["location", "date", "excess"]], on=["location", "date"], how="outer")
        df = df.assign(excess=df.excess.fillna(0))
        # Correct
        mask = df.variant.isin(["non_who"])
        df.loc[mask, "perc_sequences"] = (df.loc[mask, "perc_sequences"] - df.loc[mask, "excess"]).round(4)
        df = df.drop(columns="excess")
        # 2) `others`
        # Get excess
        x = df[-df.variant.isin(["non_who"])]
        x = x.groupby(["location", "date"], as_index=False).agg({"perc_sequences": sum})
        x = x[abs(x["perc_sequences"] - 100) != 0]
        x["excess"] = x.perc_sequences - 100
        # Merge excess quantity with input df
        df = df.merge(x[["location", "date", "excess"]], on=["location", "date"], how="outer")
        df = df.assign(excess=df.excess.fillna(0))
        # Correct
        mask = df.variant.isin(["others"])
        df.loc[mask, "perc_sequences"] = (df.loc[mask, "perc_sequences"] - df.loc[mask, "excess"]).round(4)
        df = df.drop(columns="excess")
        return df

    def pipe_out(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.columns_out].sort_values(["location", "date"])  #  + ["perc_sequences_raw"]

    def run(self, output_path: str):
        data = self.extract()
        df = self.transform(data)
        self.load(df, output_path)


def run_etl(output_path: str):
    etl = VariantsETL()
    etl.run(output_path)
