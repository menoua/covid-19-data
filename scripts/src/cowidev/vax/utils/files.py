import os
import json
from pathlib import Path

import pandas as pd
from bs4 import UnicodeDammit
from cowidev.utils import paths


STATIC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "_static"))
QUERIES_DIR = os.path.abspath(os.path.join(STATIC_DIR, "queries"))
DATA_DIR = os.path.abspath(os.path.join(STATIC_DIR, "data"))


def load_query(query_filename: str, file_ext: str = "json", to_str: bool = True):
    """Load a query from a file in vax._static folder.

    Args:
        query_filename (str): Name of the query file. If no extension is provided, {query_filename}.{file_ext} will be
                                loaded
        file_ext (str, optional): Extension of the file. Defaults to "json".

    Raises:
        FileNotFoundError: If no file is found
        ValueError: If non-supported format is provided

    Returns:
        dict: Loaded query as a string
    """
    filename_path = os.path.join(QUERIES_DIR, query_filename)
    if not os.path.isfile(filename_path):
        query_filename = f"{Path(query_filename).stem}.{file_ext}"
        filename_path = os.path.join(QUERIES_DIR, query_filename)
        if not os.path.isfile(f"{filename_path}"):
            raise FileNotFoundError(f"File {filename_path} not found")
    if file_ext == "json":
        with open(filename_path) as f:
            data = json.load(f)
    else:
        raise ValueError("Only JSON format supported")
    if to_str:
        return str(data)
    return data


def load_data(data_filename: str, file_ext: str = "csv"):
    """Load a data from a file in vax._static folder.

    Args:
        data_filename (str): Name of the data file. If no extension is provided, {query_filename}.{file_ext} will be
                                loaded
        file_ext (str, optional): Extension of the file. Defaults to "csv".

    Raises:
        FileNotFoundError: If no file is found
        ValueError: If non-supported format is provided

    Returns:
        dict: Loaded query as a string
    """
    filename_path = os.path.join(DATA_DIR, data_filename)
    if not os.path.isfile(filename_path):
        data_filename = f"{Path(data_filename).stem}.{file_ext}"
        filename_path = os.path.join(DATA_DIR, data_filename)
        if not os.path.isfile(f"{filename_path}"):
            raise FileNotFoundError(f"File {filename_path} not found")
    if file_ext == "csv":
        df = pd.read_csv(filename_path)
    else:
        raise ValueError("Only CSV format supported")
    return df


def export_metadata_manufacturer(df: pd.DataFrame, source_name: str, source_url: str):
    export_metadata(df, source_name, source_url, paths.SCRIPTS.OUTPUT_VAX_META_MANUFACT)


def export_metadata_age(df: pd.DataFrame, source_name: str, source_url: str):
    export_metadata(df, source_name, source_url, paths.SCRIPTS.OUTPUT_VAX_META_AGE)


def export_metadata(df: pd.DataFrame, source_name: str, source_url: str, output_path: str):
    if "location" not in df or "date" not in df:
        raise ValueError("df must have columns `location` and `date`.")
    df = (
        df.sort_values("date")[["location", "date"]]
        .drop_duplicates(subset=["location"], keep="last")
        .rename(columns={"date": "last_observation_date"})
        .assign(
            source_name=source_name,
            source_url=source_url,
        )
    )
    assert pd.api.types.is_string_dtype(df.last_observation_date), "`date` is not of type string in the dataframe"

    if os.path.isfile(output_path):
        df_current = pd.read_csv(output_path)
        df_current = df_current.loc[~df_current.location.isin(df.location)]
        df = pd.concat([df_current, df])

    df.sort_values("location")[["location", "last_observation_date", "source_name", "source_url"]].to_csv(
        output_path, index=False
    )


def get_file_encoding(file_path):
    with open(file_path, "rb") as file:
        content = file.read()
    suggestion = UnicodeDammit(content)
    return suggestion.original_encoding
