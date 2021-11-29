import pandas as pd

from cowidev.utils.clean import extract_clean_date, clean_count
from cowidev.utils.web import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Bolivia:
    def __init__(self):
        self.source_url = "https://www.unidoscontraelcovid.gob.bo"
        # self.source_url = "https://www.unidoscontraelcovid.gob.bo/index.php/category/reportes"
        self.location = "Bolivia"

    def read(self):
        return self._parse_data()
        # soup = get_soup(self.source_url)
        # links = self._parse_links_pdfs(soup)
        # For now, only get most recent link
        # link = links[0]
        # return self._parse_data(link)

    def _parse_data(self):
        dfs = pd.read_html(self.source_url, header=1)
        ds = dfs[0].squeeze()
        return ds

    def _parse_date(self):
        print(self.source_url)
        soup = get_soup(self.source_url)
        return extract_clean_date(soup.text, "Reporte (?:(?:V|v)acunación|COVID\-19) (\d\d\-\d\d\-20\d\d)", "%d-%m-%Y")

    # def _parse_links_pdfs(self, soup) -> list:
    #     elems = soup.findAll("a", text=re.compile("Reporte-de-vacunas-.*"))
    #     links = [elem.get("href") for elem in elems]
    #     return links

    # def _parse_data(self, url) -> pd.Series:
    #     pages = self._get_pages_relevant_pdf(url)
    #     date = self._parse_date(url)
    #     metrics = self._parse_metrics(pages[1])
    #     data = {"date": date, "source_url": url, **metrics}
    #     ds = pd.Series(data)
    #     return ds

    # def _get_pages_relevant_pdf(self, url) -> list:
    #     with tempfile.NamedTemporaryFile() as tf:
    #         with open(tf.name, mode="wb") as f:
    #             f.write(requests.get(url).content)
    #         with open(tf.name, mode="rb") as f:
    #             reader = PyPDF2.PdfFileReader(f)
    #             return [reader.getPage(0).extractText(), reader.getPage(1).extractText()]

    # def _parse_date(self, url: str) -> str:
    #     # return clean_date(text, "REPORTE DE VACUNACIÓN NACIONAL \n%d/%m/%Y\n")
    #     return extract_clean_date(url, r"https://.*(\d{1,2}_\d{1,2}_20\d\d).*\.pdf", "%d_%m_%Y")
    #     # date_str = url.split("-")[-1].split(".")[0]
    #     # return clean_date(date_str, "%d_%m_%Y")

    # def _parse_metrics(self, text: str) -> dict:
    #     fields_accepted = {
    #         "1ra. DOSIS",
    #         "2da DOSIS",
    #         "DOSIS UNICA",
    #     }
    #     numbers = [int(x) for x in text.split("\n") if x.isnumeric()]
    #     numbers_d = np.diff(numbers)
    #     idx = [i for i, x in enumerate(numbers_d) if x < 0]
    #     time_series = []
    #     for i in range(len(idx)):
    #         i_min = idx[i - 1] + 1
    #         i_max = idx[i] + 1
    #         if i == 0:
    #             time_series.append(numbers[:i_max])
    #         elif i == len(idx):
    #             time_series.append(numbers[i_min:])
    #         else:
    #             time_series.append(numbers[i_min:i_max])
    #     time_series = list(filter(lambda x: len(x) > 10, time_series))
    #     labels = [x for x in text.split("\n") if "DOSIS" in x]

    #     if len(time_series) != 3 or len(labels) != 3:
    #         raise ValueError(
    #             "Not three time series detected. Only first, second and unique dose time series expected."
    #         )
    #     fields_wrong = fields_accepted.difference(labels)
    #     if fields_wrong:
    #         raise ValueError(f"Invalid field found: {fields_wrong}")
    #     metrics = [max(x) for x in time_series]
    #     return dict(zip(labels, metrics))

    def pipe_checks(self, ds: pd.Series) -> pd.Series:
        unknown_cols = ds.index.difference(["PRIMERA DOSIS", "SEGUNDA DOSIS", "UNIDOSIS", "date"])
        if unknown_cols.any():
            raise ValueError(f"Unknown columns {unknown_cols}")
        return ds

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        ds.loc["date"] = self._parse_date()
        return ds

    def pipe_metrics(self, ds: pd.Series) -> pd.Series:
        cols = ["PRIMERA DOSIS", "SEGUNDA DOSIS", "UNIDOSIS"]
        ds.loc[cols] = ds.loc[cols].apply(clean_count)
        ds = ds.rename(
            {
                "PRIMERA DOSIS": "people_vaccinated",
                "SEGUNDA DOSIS": "people_fully_vaccinated",
            }
        )
        ds = enrich_data(ds, "total_vaccinations", ds.people_vaccinated + ds.people_fully_vaccinated + ds["UNIDOSIS"])
        ds.people_vaccinated = ds.people_vaccinated + ds["UNIDOSIS"]
        ds.people_fully_vaccinated = ds.people_fully_vaccinated + ds["UNIDOSIS"]
        ds = ds.drop("UNIDOSIS")
        return ds

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Johnson&Johnson, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V",
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds.pipe(self.pipe_checks)
            .pipe(self.pipe_date)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_source)
            .pipe(self.pipe_location)
        )

    def export(self):
        """Generalized."""
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
    Bolivia().export()


if __name__ == "__main__":
    main()
