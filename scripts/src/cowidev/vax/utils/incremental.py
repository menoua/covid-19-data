import os
import datetime
import re
import numbers

import pandas as pd
import requests
from cowidev.utils import paths


GH_LINK = "https://github.com/owid/covid-19-data/raw/master/public/data/vaccinations/country_data"


def enrich_data(ds: pd.Series, row, value) -> pd.Series:
    return ds.append(pd.Series({row: value}))


def increment(
    location,
    total_vaccinations,
    date,
    vaccine,
    source_url,
    people_vaccinated=None,
    people_partly_vaccinated=None,
    people_fully_vaccinated=None,
    total_boosters=None,
):
    # Check fields
    _check_fields(
        location=location,
        vaccine=vaccine,
        source_url=source_url,
        date=date,
        total_vaccinations=total_vaccinations,
        people_vaccinated=people_vaccinated,
        people_partly_vaccinated=people_partly_vaccinated,
        people_fully_vaccinated=people_fully_vaccinated,
        total_boosters=total_boosters,
    )
    _from_gh_to_scripts(location)
    filepath_automated = paths.out_vax(location)
    # Update file in output/
    if os.path.isfile(filepath_automated):
        df = _increment(
            filepath=filepath_automated,
            location=location,
            total_vaccinations=total_vaccinations,
            date=date,
            vaccine=vaccine,
            source_url=source_url,
            people_vaccinated=people_vaccinated,
            people_partly_vaccinated=people_partly_vaccinated,
            people_fully_vaccinated=people_fully_vaccinated,
            total_boosters=total_boosters,
        )
    # If not available, create new file
    else:
        df = _build_df(
            location=location,
            total_vaccinations=total_vaccinations,
            date=date,
            vaccine=vaccine,
            source_url=source_url,
            people_vaccinated=people_vaccinated,
            people_partly_vaccinated=people_partly_vaccinated,
            people_fully_vaccinated=people_fully_vaccinated,
            total_boosters=total_boosters,
        )
    # To Integer type
    col_ints = [
        "total_vaccinations",
        "people_vaccinated",
        "people_partly_vaccinated",
        "people_fully_vaccinated",
        "total_boosters",
    ]
    for col in col_ints:
        if col in df.columns:
            df[col] = df[col].astype("Int64").fillna(pd.NA)

    df.to_csv(paths.out_vax(location), index=False)
    # print(f"NEW: {total_vaccinations} doses on {date}")


def _from_gh_to_scripts(location):
    filepath_automated = paths.out_vax(location)
    filepath_public = f"{GH_LINK}/{location}.csv".replace(" ", "%20")
    # Move from public to output folder
    if not os.path.isfile(filepath_automated) and requests.get(filepath_public).ok:
        pd.read_csv(filepath_public).to_csv(filepath_automated, index=False)


def _check_fields(
    location,
    source_url,
    vaccine,
    date,
    total_vaccinations,
    people_vaccinated,
    people_partly_vaccinated,
    people_fully_vaccinated,
    total_boosters,
):
    # Check location, vaccine, source_url
    if not isinstance(location, str):
        type_wrong = type(location).__name__
        raise TypeError(f"Check `location` type! Should be a str, found {type_wrong}. Value was {location}")
    if not isinstance(vaccine, str):
        type_wrong = type(vaccine).__name__
        raise TypeError(f"Check `vaccine` type! Should be a str, found {type_wrong}. Value was {vaccine}")
    if not isinstance(source_url, str):
        type_wrong = type(source_url).__name__
        raise TypeError(f"Check `source_url` type! Should be a str, found {type_wrong}. Value was {source_url}")

    # Check metrics
    if not isinstance(total_vaccinations, numbers.Number):
        type_wrong = type(location).__name__
        raise TypeError(
            f"Check `total_vaccinations` type! Should be numeric, found {type_wrong}. Value was {total_vaccinations}"
        )
    if not (isinstance(total_boosters, numbers.Number) or pd.isnull(total_boosters)):
        type_wrong = type(total_boosters).__name__
        raise TypeError(
            f"Check `total_boosters` type! Should be numeric, found {type_wrong}. Value was {total_boosters}"
        )
    if not (isinstance(people_vaccinated, numbers.Number) or pd.isnull(people_vaccinated)):
        type_wrong = type(people_vaccinated).__name__
        raise TypeError(
            f"Check `people_vaccinated` type! Should be numeric, found {type_wrong}. Value was {people_vaccinated}"
        )
    if not (isinstance(people_partly_vaccinated, numbers.Number) or pd.isnull(people_partly_vaccinated)):
        type_wrong = type(people_partly_vaccinated).__name__
        raise TypeError(
            f"Check `people_vaccinated` type! Should be numeric, found {type_wrong}. Value was {people_vaccinated}"
        )
    if not (isinstance(people_fully_vaccinated, numbers.Number) or pd.isnull(people_fully_vaccinated)):
        type_wrong = type(people_fully_vaccinated).__name__
        raise TypeError(
            f"Check `people_fully_vaccinated` type! Should be numeric, found {type_wrong}. Value was "
            f"{people_fully_vaccinated}"
        )
    # Check date
    if not isinstance(date, str):
        type_wrong = type(date).__name__
        raise TypeError(f"Check `date` type! Should be numeric, found {type_wrong}. Value was {date}")
    if not (re.match(r"\d{4}-\d{2}-\d{2}", date) and date <= str(datetime.date.today() + datetime.timedelta(days=1))):
        raise ValueError(f"Check `date`. It either does not match format YYYY-MM-DD or exceeds todays'date: {date}")


def _increment(
    filepath,
    location,
    total_vaccinations,
    date,
    vaccine,
    source_url,
    people_vaccinated=None,
    people_partly_vaccinated=None,
    people_fully_vaccinated=None,
    total_boosters=None,
):
    prev = pd.read_csv(filepath)
    if total_vaccinations <= prev["total_vaccinations"].max() or date < prev["date"].max():
        df = prev.copy()
    elif date == prev["date"].max():
        df = prev.copy()
        df.loc[df["date"] == date, "total_vaccinations"] = total_vaccinations
        df.loc[df["date"] == date, "people_vaccinated"] = people_vaccinated
        if people_partly_vaccinated is not None:
            df.loc[df["date"] == date, "people_partly_vaccinated"] = people_partly_vaccinated
        df.loc[df["date"] == date, "people_fully_vaccinated"] = people_fully_vaccinated
        df.loc[df["date"] == date, "total_boosters"] = total_boosters
        df.loc[df["date"] == date, "source_url"] = source_url
    else:
        new = _build_df(
            location,
            total_vaccinations,
            date,
            vaccine,
            source_url,
            people_vaccinated,
            people_partly_vaccinated,
            people_fully_vaccinated,
            total_boosters,
        )
        df = pd.concat([prev, new])
    return df.sort_values("date")


def _build_df(
    location,
    total_vaccinations,
    date,
    vaccine,
    source_url,
    people_vaccinated=None,
    people_partly_vaccinated=None,
    people_fully_vaccinated=None,
    total_boosters=None,
):
    new = pd.DataFrame(
        {
            "location": location,
            "date": date,
            "vaccine": vaccine,
            "total_vaccinations": [total_vaccinations],
            "source_url": source_url,
        }
    )
    if people_vaccinated is not None:
        new["people_vaccinated"] = people_vaccinated
    if people_partly_vaccinated is not None:
        new["people_partly_vaccinated"] = people_partly_vaccinated
    if people_fully_vaccinated is not None:
        new["people_fully_vaccinated"] = people_fully_vaccinated
    if total_boosters is not None:
        new["total_boosters"] = total_boosters
    return new


def merge_with_current_data(df: pd.DataFrame, filepath: str) -> pd.DataFrame:
    col_ints = [
        "total_vaccinations",
        "people_vaccinated",
        "people_partly_vaccinated",
        "people_fully_vaccinated",
        "total_boosters",
    ]
    # Load current data
    if os.path.isfile(filepath):
        df_current = pd.read_csv(filepath)
        # Merge
        df_current = df_current[~df_current.date.isin(df.date)]
        df = pd.concat([df, df_current]).sort_values(by="date")
        # Int values
    col_ints = list(df.columns.intersection(col_ints))
    if col_ints:
        df[col_ints] = df[col_ints].astype("Int64").fillna(pd.NA)

    columns = ["location", "date", "vaccine", "source_url"] + col_ints
    return df[columns]
