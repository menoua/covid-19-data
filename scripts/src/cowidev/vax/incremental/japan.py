import pandas as pd

from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    return connect_parse_data(source)


def connect_parse_data(source: str) -> pd.Series:

    soup = get_soup(source)

    df = pd.read_html(str(soup.find(class_="vaccination-count")))[0]
    assert df.shape == (3, 7)

    values = df.iloc[:, 2].values

    total_vaccinations = values[0]
    people_vaccinated = values[1]
    people_fully_vaccinated = values[2]
    assert total_vaccinations == people_vaccinated + people_fully_vaccinated

    date = soup.find(class_="aly_tx_center").text
    date = localdate("Asia/Tokyo")

    data = {
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Japan")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Moderna, Pfizer/BioNTech, Oxford/AstraZeneca")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://www.kantei.go.jp/jp/headline/kansensho/vaccine.html")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main():
    source = "https://www.kantei.go.jp/jp/headline/kansensho/vaccine.html"
    data = read(source).pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )


if __name__ == "__main__":
    main()
