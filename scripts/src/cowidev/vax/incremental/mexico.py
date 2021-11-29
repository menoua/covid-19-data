import tempfile
import re

import pandas as pd
import requests
import PyPDF2

from cowidev.utils.clean import clean_date, clean_count
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Mexico:
    def __init__(self):
        self.source_page = "https://www.gob.mx/salud/documentos/presentaciones-de-las-conferencias-de-prensa-2021"
        self.location = "Mexico"

    def read(self):
        soup = get_soup(self.source_page)
        link = self._parse_link_pdf(soup)
        return self._parse_data(link)

    def _parse_link_pdf(self, soup) -> list:
        link = soup.find(class_="list-unstyled").find("a")["href"]
        link = "http://www.gob.mx" + link
        self.source_url = link
        return link

    def _get_pages_relevant_pdf(self, url) -> list:
        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, mode="wb") as f:
                f.write(requests.get(url).content)
            with open(tf.name, mode="rb") as f:
                reader = PyPDF2.PdfFileReader(f)
                return [reader.getPage(i).extractText() for i in range(10)]

    def _parse_data(self, url) -> pd.Series:
        pages = self._get_pages_relevant_pdf(url)
        for page_text in pages:
            page_text = re.sub(r"\s+", " ", page_text)

            if "Total de dosis aplicadas reportadas" in page_text:
                total_vaccinations = clean_count(
                    re.search(r"([\d,]{10,}) ?Total de dosis aplicadas reportadas", page_text).group(1)
                )
                date = clean_date(
                    re.search(r"corte de informaci.n al (\d+ \w+ 202\d)", page_text).group(1),
                    fmt="%d %B %Y",
                    lang="es",
                )

            elif "Personas vacunadas reportadas" in page_text:
                people_vaccinated = clean_count(re.search(r"Personas vacunadas \*\s?([\d,]{10,})", page_text).group(1))
                people_fully_vaccinated = clean_count(re.search(r"Esquema completo ([\d,]{10,})", page_text).group(1))

        # Tests
        assert total_vaccinations >= 94300526
        assert people_vaccinated >= 61616895
        assert people_fully_vaccinated >= 41115211
        assert people_vaccinated <= total_vaccinations
        assert people_fully_vaccinated >= 0.5 * people_vaccinated
        assert date >= "2021-09-16"

        return pd.Series(
            {
                "total_vaccinations": total_vaccinations,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "date": date,
            }
        )

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Johnson&Johnson, Oxford/AstraZeneca, Moderna, Pfizer/BioNTech, Sinovac, Sputnik V, CanSino",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

    def export(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    Mexico().export()


if __name__ == "__main__":
    main()
