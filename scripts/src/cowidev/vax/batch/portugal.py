import pandas as pd

from cowidev.vax.utils.utils import make_monotonic
from cowidev.utils import paths


def read(source_url: str) -> pd.DataFrame:
    return pd.read_csv(
        source_url,
        usecols=[
            "data",
            "vacinas",
            "pessoas_vacinadas_completamente",
            "pessoas_vacinadas_parcialmente",
        ],
    )


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "data": "date",
            "vacinas": "total_vaccinations",
            "pessoas_vacinadas_completamente": "people_fully_vaccinated",
        }
    )


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=pd.to_datetime(df.date, format="%d-%m-%Y").astype(str))


def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["total_vaccinations"]).assign(
        people_vaccinated=df.pessoas_vacinadas_parcialmente + df.people_fully_vaccinated
    )
    return df[["date", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]]


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(date: str) -> str:
        if date >= "2021-04-26":
            return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        if date >= "2021-02-09":
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        return "Pfizer/BioNTech"

    return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))


def enrich_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Portugal", source_url="https://github.com/dssg-pt/covid19pt-data")


def add_boosters(df: pd.DataFrame) -> pd.DataFrame:
    # Booster data is only reported as rounded values in reports or press conference. No booster
    # data is available from the source as of Nov 26 2021, but Rui Barros from Publico is collecting
    # the data on GitHub.
    boosters = (
        pd.read_csv("https://raw.githubusercontent.com/ruimgbarros/vacinacao/master/booster_doses.csv")
        .rename(columns={"Data": "date", "n_doses_booster": "total_boosters"})
        .assign(location="Portugal")
    )
    boosters["date"] = boosters.date.str.slice(0, 10)
    df = pd.merge(df, boosters, how="outer", on=["date", "location"], validate="one_to_one").sort_values("date")
    df.loc[df.source_url.isna(), "source_url"] = "https://github.com/ruimgbarros/vacinacao"
    return df


def sanity_checks(df: pd.DataFrame) -> pd.DataFrame:
    assert all(df.total_vaccinations.fillna(0) >= df.people_vaccinated.fillna(0))
    return df


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(rename_columns)
        .pipe(format_date)
        .pipe(calculate_metrics)
        .pipe(enrich_columns)
        .pipe(add_boosters)
        .pipe(enrich_vaccine_name)
        .pipe(sanity_checks)
        .pipe(make_monotonic)
        .sort_values("date")
    )


def main():
    source_url = "https://github.com/dssg-pt/covid19pt-data/raw/master/vacinas.csv"
    destination = paths.out_vax("Portugal")
    read(source_url).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
