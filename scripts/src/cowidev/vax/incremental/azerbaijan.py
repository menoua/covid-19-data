import tempfile
import re

import requests
import pandas as pd
from bs4 import BeautifulSoup
import PyPDF2
from pdfreader import SimplePDFViewer

from cowidev.utils.clean import clean_date, clean_count
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str):
    soup = get_soup(source)
    url = parse_pdf_link(soup, source)
    if not url.endswith(".pdf"):
        raise ValueError(f"File reporting metrics is not a PDF: {url}!")
    ds = pd.Series(parse_data(url))
    return ds


def parse_pdf_link(soup: BeautifulSoup, source: str):
    href = soup.find("a", string="Vaksinasiya").get("href")
    return f"{source}{href}"


def parse_data(source_pdf: str):
    with tempfile.NamedTemporaryFile() as tf:
        with open(tf.name, mode="wb") as f:
            f.write(requests.get(source_pdf).content)
        (
            total_vaccinations,
            people_vaccinated,
            people_fully_vaccinated,
            total_boosters,
        ) = parse_vaccinations(tf.name)
        date = parse_date(tf.name)
    return {
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "total_boosters": total_boosters,
        "date": date,
    }


def parse_date(filename):
    # Read pdf (for date)
    with open(filename, mode="rb") as f:
        reader = PyPDF2.PdfFileReader(f)
        page = reader.getPage(0)
        text = page.extractText()
    # Get date
    date_str = re.search(r"\n(?P<count>\d{1,2}.\d{1,2}.\d{4})\n", text).group(1)
    return clean_date(date_str, "%d.%m.%Y")


def parse_vaccinations(filename):
    # Read pdf (for metrics)
    with open(filename, mode="rb") as f:
        viewer = SimplePDFViewer(f)
        viewer.render()
    # Get list with strings
    strs = viewer.canvas.strings
    # Infer figures
    numbers = []
    for str in strs:
        try:
            numbers.append(clean_count(str))
        except:
            pass
    numbers.sort()
    total_vaccinations = numbers[-1]
    people_vaccinated = numbers[-2]
    people_fully_vaccinated = numbers[-3]
    total_boosters = numbers[-4]
    # Sanity check
    if people_vaccinated + people_fully_vaccinated + total_boosters != total_vaccinations:
        raise ValueError(
            f"people_vaccinated + people_fully_vaccinated + total_boosters != total_vaccinations ({people_vaccinated} + {people_fully_vaccinated} + {total_boosters} != {total_vaccinations})"
        )
    return total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Azerbaijan")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac, Sputnik V")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return df.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source, source)


def main():
    source = "https://koronavirusinfo.az"
    data = read(source).pipe(pipeline, source)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        total_boosters=data["total_boosters"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )


if __name__ == "__main__":
    main()
