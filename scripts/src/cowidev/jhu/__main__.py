import argparse
import os
import sys
import pandas as pd
import numpy as np
import pytz
from datetime import datetime
from termcolor import colored
from cowidev.utils.s3 import df_to_s3

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

from cowidev.utils.utils import get_project_dir
from cowidev.megafile.generate import generate_megafile
from cowidev.jhu.shared import (
    load_population,
    load_owid_continents,
    inject_owid_aggregates,
    inject_per_million,
    inject_days_since,
    inject_cfr,
    inject_population,
    inject_rolling_avg,
    inject_exemplars,
    inject_doubling_days,
    inject_weekly_growth,
    inject_biweekly_growth,
    standard_export,
    ZERO_DAY,
)
from cowidev.grapher.db.utils.slack_client import send_warning, send_success
from cowidev.grapher.db.utils.db_imports import import_dataset


INPUT_PATH = os.path.join(get_project_dir(), "scripts", "input", "jhu")
OUTPUT_PATH = os.path.join(get_project_dir(), "public", "data", "jhu")
TMP_PATH = os.path.join(get_project_dir(), "scripts", "tmp")
LOCATIONS_CSV_PATH = os.path.join(INPUT_PATH, "jhu_country_standardized.csv")

ERROR = colored("[Error]", "red")
WARNING = colored("[Warning]", "yellow")

DATASET_NAME = "COVID-19 - Johns Hopkins University"

LARGE_DATA_CORRECTIONS = [
    ("Brazil", "2021-09-18", "cases"),
    ("Ecuador", "2020-09-07", "deaths"),
    ("Ecuador", "2021-07-20", "deaths"),
    ("France", "2021-05-20", "cases"),
    ("Mexico", "2021-06-01", "deaths"),
    ("Turkey", "2020-12-10", "cases"),
]


def print_err(*args, **kwargs):
    return print(*args, file=sys.stderr, **kwargs)


def download_csv():
    files = ["time_series_covid19_confirmed_global.csv", "time_series_covid19_deaths_global.csv"]
    for file in files:
        print(file)
        os.system(
            f"curl --silent -f -o {INPUT_PATH}/{file} -L"
            f" https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/{file}"
        )


def get_metric(metric, region):
    file_path = os.path.join(INPUT_PATH, f"time_series_covid19_{metric}_{region}.csv")
    df = pd.read_csv(file_path).drop(columns=["Lat", "Long"])

    if metric == "confirmed":
        metric = "total_cases"
    elif metric == "deaths":
        metric = "total_deaths"
    else:
        print_err("Unknown metric requested.\n")
        sys.exit(1)

    # Relabel as 'International'
    df.loc[df["Country/Region"].isin(["Diamond Princess", "MS Zaandam"]), "Country/Region"] = "International"

    # Relabel Hong Kong to its own time series
    df.loc[df["Province/State"] == "Hong Kong", "Country/Region"] = "Hong Kong"

    national = df.drop(columns="Province/State").groupby("Country/Region", as_index=False).sum()

    df = national.copy()  # df = pd.concat([national, subnational]).reset_index(drop=True)
    df = df.melt(id_vars="Country/Region", var_name="date", value_name=metric)
    df.loc[:, "date"] = pd.to_datetime(df["date"], format="%m/%d/%y").dt.date
    df = df.sort_values("date")

    # Only start country series when total_cases > 0 or total_deaths > 0 to minimize file size
    cutoff = (
        df.loc[df[metric] == 0, ["date", "Country/Region"]]
        .groupby("Country/Region", as_index=False)
        .max()
        .rename(columns={"date": "cutoff"})
    )
    df = df.merge(cutoff, on="Country/Region", how="left")
    df = df[(df.date >= df.cutoff) | (df.cutoff.isna())].drop(columns="cutoff")

    df.loc[:, metric.replace("total_", "new_")] = df[metric] - df.groupby("Country/Region")[metric].shift(1)
    return df


def load_data():
    global_cases = get_metric("confirmed", "global")
    global_deaths = get_metric("deaths", "global")
    return pd.merge(global_cases, global_deaths, on=["date", "Country/Region"], how="outer")


def load_locations():
    return pd.read_csv(LOCATIONS_CSV_PATH, keep_default_na=False).rename(
        columns={"Country": "Country/Region", "Our World In Data Name": "location"}
    )


def _load_merged():
    df_data = load_data()
    df_locs = load_locations()
    return df_data.merge(df_locs, how="left", on=["Country/Region"])


def check_data_correctness(df_merged):
    errors = 0

    # Check that every country name is standardized
    df_uniq = df_merged[["Country/Region", "location"]].drop_duplicates()
    if df_uniq["location"].isnull().any():
        print_err("\n" + ERROR + " Could not find OWID names for:")
        print_err(df_uniq[df_uniq["location"].isnull()])
        errors += 1

    # Drop missing locations for the further checks – that error is addressed above
    df_merged = df_merged.dropna(subset=["location"])

    # Check for duplicate rows
    if df_merged.duplicated(subset=["date", "location"]).any():
        print_err("\n" + ERROR + " Found duplicate rows:")
        print_err(df_merged[df_merged.duplicated(subset=["date", "location"])])
        errors += 1

    # Check for missing population figures
    df_pop = load_population()
    pop_entity_diff = (
        set(df_uniq["location"]) - set(df_pop["location"]) - set(["International", "Summer Olympics 2020"])
    )
    if len(pop_entity_diff) > 0:
        # this is not an error, so don't increment errors variable
        print("\n" + WARNING + " These entities were not found in the population dataset:")
        print(pop_entity_diff)
        print()
        formatted_msg = ", ".join(f"`{entity}`" for entity in pop_entity_diff)
        send_warning(
            channel="corona-data-updates",
            title="Some entities are missing from the population dataset",
            message=formatted_msg,
        )

    return errors == 0


def discard_rows(df):
    for ldc in LARGE_DATA_CORRECTIONS:
        df.loc[(df.location == ldc[0]) & (df.date.astype(str) == ldc[1]), f"new_{ldc[2]}"] = np.nan
    return df


def load_standardized(df):
    df = df[["date", "location", "new_cases", "new_deaths", "total_cases", "total_deaths"]]
    df = discard_rows(df)
    df = inject_owid_aggregates(df)
    df = inject_weekly_growth(df)
    df = inject_biweekly_growth(df)
    df = inject_doubling_days(df)
    df = inject_per_million(
        df,
        [
            "new_cases",
            "new_deaths",
            "total_cases",
            "total_deaths",
            "weekly_cases",
            "weekly_deaths",
            "biweekly_cases",
            "biweekly_deaths",
        ],
    )
    df = inject_rolling_avg(df)
    df = inject_cfr(df)
    df = inject_days_since(df)
    df = inject_exemplars(df)
    return df.sort_values(by=["location", "date"])


def export(df_merged):
    df_loc = df_merged[["Country/Region", "location"]].drop_duplicates()
    df_loc = df_loc.merge(load_owid_continents(), on="location", how="left")
    df_loc = inject_population(df_loc)
    df_loc["population_year"] = df_loc["population_year"].round().astype("Int64")
    df_loc["population"] = df_loc["population"].round().astype("Int64")
    df_loc = df_loc.sort_values("location")
    df_loc.to_csv(os.path.join(OUTPUT_PATH, "locations.csv"), index=False)
    # The rest of the CSVs
    return standard_export(load_standardized(df_merged), OUTPUT_PATH, DATASET_NAME)


def clean_global_subnational(metric):
    url = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_{metric}_global.csv"
    metric = "cases" if metric == "confirmed" else "deaths"

    df = (
        pd.read_csv(url, na_values="")
        .drop(columns=["Lat", "Long"])
        .dropna(subset=["Province/State"])
        .melt(id_vars=["Country/Region", "Province/State"], var_name="date", value_name=f"total_{metric}")
        .rename(columns={"Country/Region": "location1", "Province/State": "location2"})
    )
    df["date"] = pd.to_datetime(df.date).dt.date.astype(str)
    df = df.sort_values(["location1", "location2", "date"])
    df[f"new_{metric}"] = df[f"total_{metric}"] - df.groupby(["location1", "location2"])[f"total_{metric}"].shift(1)
    df[f"new_{metric}_smoothed"] = (
        df.groupby(["location1", "location2"]).rolling(7)[f"new_{metric}"].mean().droplevel(level=[0, 1]).round(2)
    )
    df["location3"] = pd.NA
    return df


def clean_us_subnational(metric):
    url = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_{metric}_US.csv"
    metric = "cases" if metric == "confirmed" else "deaths"

    df = (
        pd.read_csv(url)
        .drop(
            columns=[
                "UID",
                "iso2",
                "iso3",
                "code3",
                "FIPS",
                "Country_Region",
                "Lat",
                "Long_",
                "Combined_Key",
                "Population",
            ],
            errors="ignore",
        )
        .melt(id_vars=["Province_State", "Admin2"], var_name="date", value_name=f"total_{metric}")
        .rename(columns={"Province_State": "location2", "Admin2": "location3"})
    )
    df["date"] = pd.to_datetime(df.date).dt.date.astype(str)
    df = df.sort_values(["location2", "location3", "date"])
    df[f"new_{metric}"] = df[f"total_{metric}"] - df.groupby(["location2", "location3"])[f"total_{metric}"].shift(1)
    df[f"new_{metric}_smoothed"] = (
        df.groupby(["location2", "location3"]).rolling(7)[f"new_{metric}"].mean().droplevel(level=[0, 1]).round(2)
    )
    df["location1"] = "United States"
    return df


def create_subnational():
    global_cases = clean_global_subnational("confirmed")
    global_deaths = clean_global_subnational("deaths")
    us_cases = clean_us_subnational("confirmed")
    us_deaths = clean_us_subnational("deaths")

    df = pd.concat(
        [
            pd.merge(global_cases, global_deaths, on=["location1", "location2", "location3", "date"], how="outer"),
            pd.merge(us_cases, us_deaths, on=["location1", "location2", "location3", "date"], how="outer"),
        ]
    ).sort_values(["location1", "location2", "location3", "date"])[
        [
            "location1",
            "location2",
            "location3",
            "date",
            "total_cases",
            "new_cases",
            "new_cases_smoothed",
            "total_deaths",
            "new_deaths",
            "new_deaths_smoothed",
        ]
    ]
    df = df[df.total_cases > 0]
    filename = "subnational_cases_deaths"
    compression = {"method": "zip", "archive_name": f"{filename}.csv"}
    # df.to_csv(os.path.join(OUTPUT_PATH, f"{filename}.zip"), index=False, compression=compression)
    df_to_s3(df, f"public/jhu/{filename}.zip", extension="csv", compression=compression, public=True)


def main(skip_download=False):

    if not skip_download:
        print("\nAttempting to download latest CSV files...")
        download_csv()

    df_merged = _load_merged()

    if check_data_correctness(df_merged):
        print("Data correctness check %s.\n" % colored("passed", "green"))
    else:
        print_err("Data correctness check %s.\n" % colored("failed", "red"))
        sys.exit(1)

    if export(df_merged):
        print("Successfully exported CSVs to %s\n" % colored(os.path.abspath(OUTPUT_PATH), "magenta"))
    else:
        print_err("JHU export failed.\n")
        sys.exit(1)

    print("Generating megafile…")
    generate_megafile()
    print("Megafile is ready.")

    send_success(channel="corona-data-updates", title="Updated JHU GitHub exports")

    print("Generating subnational file…")
    create_subnational()


def update_db():
    time_str = datetime.now().astimezone(pytz.timezone("Europe/London")).strftime("%-d %B, %H:%M")
    source_name = f"Johns Hopkins University CSSE COVID-19 Data – Last updated {time_str} (London time)"
    import_dataset(
        dataset_name=DATASET_NAME,
        namespace="owid",
        csv_path=os.path.join(OUTPUT_PATH, DATASET_NAME + ".csv"),
        default_variable_display={"yearIsDay": True, "zeroDay": ZERO_DAY},
        source_name=source_name,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run JHU update script")
    parser.add_argument(
        "-s",
        "--skip-download",
        action="store_true",
        help="Skip downloading files from the JHU repository",
    )
    args = parser.parse_args()
    main(skip_download=args.skip_download)
