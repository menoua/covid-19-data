import os
import re
import numbers
import datetime

import pandas as pd
from cowidev.utils import paths


UNITS_ACCEPTED = {"people tested", "samples tested", "tests performed", "units unclear", "tests performed (CDC)"}


def increment(
    count: int,
    sheet_name: str,
    country: str,
    units: str,
    date: str,
    source_url: str,
    source_label: str,
    testing_type=None,
    notes=None,
    daily_change=None,
):
    # Read current dataframe
    output_path = os.path.join(paths.SCRIPTS.OLD, "testing", "automated_sheets", f"{sheet_name}.csv")
    df_current = pd.read_csv(output_path)

    # Sanity checks
    _check_fields(df_current, country, source_url, source_label, units, date, count)

    # Create new df
    df = pd.DataFrame(
        [
            {
                "Country": country,
                "Units": units,
                "Date": date,
                "Cumulative total": count,
                "Source URL": source_url,
                "Source label": source_label,
                "Notes": notes,
            }
        ]
    )
    if daily_change is not None:
        df["Daily change in cumulative total"] = daily_change

    # Merge
    df_current = df_current[df_current.Date != date]
    df = pd.concat([df_current, df])
    df = df.sort_values("Date")
    df = df.drop_duplicates(subset=["Cumulative total"], keep="first")
    # Export
    df.to_csv(output_path, index=False)


def _check_fields(
    df_current: str,
    location: str,
    source_url: str,
    source_label: str,
    units: str,
    date,
    cumulative_total: numbers.Number,
):
    # Check location, vaccine, source_url
    if not isinstance(location, str):
        type_wrong = type(location).__name__
        raise TypeError(f"Check `location` type! Should be a str, found {type_wrong}. Value was {location}")
    if not isinstance(source_label, str):
        type_wrong = type(source_label).__name__
        raise TypeError(f"Check `source_label` type! Should be a str, found {type_wrong}. Value was {source_label}")
    if not isinstance(source_url, str):
        type_wrong = type(source_url).__name__
        raise TypeError(f"Check `source_url` type! Should be a str, found {type_wrong}. Value was {source_url}")
    if not isinstance(units, str):
        type_wrong = type(units).__name__
        raise TypeError(f"Check `units` type! Should be a str, found {type_wrong}. Value was {units}")
    if units not in UNITS_ACCEPTED:
        raise ValueError(f"Value for `units` is not accepted ({units}). Should be one of {UNITS_ACCEPTED}")

    # Check metrics
    if not isinstance(cumulative_total, numbers.Number):
        type_wrong = type(location).__name__
        raise TypeError(
            f"Check `cumulative_total` type! Should be numeric, found {type_wrong}. Value was {cumulative_total}"
        )
    if df_current["Cumulative total"].max() > cumulative_total:
        raise ValueError(f"`cumulative_total` can't be lower than currently highers 'Cumulative total' value.")

    # Check date
    if not isinstance(date, str):
        type_wrong = type(date).__name__
        raise TypeError(f"Check `date` type! Should be numeric, found {type_wrong}. Value was {date}")
    if not (re.match(r"\d{4}-\d{2}-\d{2}", date) and date <= str(datetime.date.today() + datetime.timedelta(days=1))):
        raise ValueError(f"Check `date`. It either does not match format YYYY-MM-DD or exceeds todays'date: {date}")
