import re
import requests
import tempfile
import itertools

from bs4 import BeautifulSoup
import pandas as pd
import PyPDF2

from cowidev.utils.clean import clean_count, clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import increment


vaccines_mapping = {
    "Covishield Vaccine": "Oxford/AstraZeneca",
    "AstraZeneca Vaccine": "Oxford/AstraZeneca",
    "Sinopharm Vaccine": "Sinopharm/Beijing",
    "Sputnik V": "Sputnik V",
    "Pfizer": "Pfizer/BioNTech",
    "Moderna": "Moderna",
}

regex_mapping = {
    "Covishield Vaccine": r"(Covishield Vaccine) 1st Dose ([\d,]+) 2nd Dose ([\d,]+)",
    "AstraZeneca Vaccine": r"(AstraZeneca Vaccine) 1st Dose ([\d,]+) 2nd Dose ([\d,]+)",
    "Sinopharm Vaccine": r"(Sinopharm Vaccine) 1st Dose ([\d,]+) 2nd Dose ([\d,]+)",
    "Sputnik V": r"(Sputnik V) 1st Dose ([\d,]+) 2nd Dose ([\d,]+)",
    "Pfizer": r"(Pfizer) 1st Dose ([\d,]+) 2nd Dose ([\d,]+)",
    "Moderna": r"(Moderna) 1st Dose ([\d,]+) 2nd Dose ([\d,]+)",
}


class SriLanka:
    def __init__(self):
        self.source_url = "https://www.epid.gov.lk/web/index.php?option=com_content&view=article&id=225&lang=en"
        self.location = "Sri Lanka"

    def read(self):
        soup = get_soup(self.source_url)
        data = self.parse_data(soup)
        # print(data)
        return pd.Series(data=data)

    def parse_data(self, soup: BeautifulSoup) -> pd.Series:
        # Get path to newest pdf
        pdf_path = self._parse_last_pdf_link(soup)
        # Get text from pdf
        text = self._extract_text_from_pdf(pdf_path)
        # Get vaccine table from text
        df_vax = self._parse_vaccines_table_as_df(text)
        people_vaccinated = df_vax.doses_1.sum()
        people_fully_vaccinated = df_vax.doses_2.sum()
        total_vaccinations = people_vaccinated + people_fully_vaccinated
        vaccine = ", ".join(df_vax.vaccine.map(vaccines_mapping))
        # Get date
        regex = r"Situation Report\s+([\d\.]{10})"
        date = re.search(regex, text).group(1)
        date = clean_date(date, "%d.%m.%Y")
        # Build data series
        return {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": date,
            "source_url": pdf_path,
            "vaccine": vaccine,
            "location": self.location,
        }

    def _parse_last_pdf_link(self, soup):
        links = soup.find(class_="rt-article").find_all("a")
        for link in links:
            if "sitrep-sl-en" in link["href"]:
                pdf_path = "https://www.epid.gov.lk" + link["href"]
                break
        if not pdf_path:
            raise ValueError("No link to PDF file was found!")
        return pdf_path

    def _extract_text_from_pdf(self, pdf_path):
        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, mode="wb") as f:
                f.write(requests.get(pdf_path).content)
            with open(tf.name, mode="rb") as f:
                reader = PyPDF2.PdfFileReader(f)
                page = reader.getPage(0)
                text = page.extractText().replace("\n", "")
        return text

    def _parse_vaccines_table_as_df(self, text):
        # Extract doses relevant sentence
        regex = r"COVID-19 Vaccination (.*) District"  # Country(/Region)? Cumulative Cases"
        vax_info = re.search(regex, text).group(1).strip().replace("No", "")
        vax_info = re.sub("\s+", " ", vax_info)
        # Sentence to DataFrame
        allresults = []
        for vaccine_regex in regex_mapping.values():
            results = re.findall(vaccine_regex, vax_info, re.IGNORECASE)
            allresults.append(results)
        flat_ls = list(itertools.chain(*allresults))
        df = pd.DataFrame(flat_ls, columns=["vaccine", "doses_1", "doses_2"]).replace("-", 0)
        df = df.replace(to_replace=[None], value=0)
        df = df.assign(
            doses_1=df["doses_1"].astype(str).apply(clean_count),
            doses_2=df["doses_2"].astype(str).apply(clean_count),
            vaccine=df.vaccine.str.strip(),
        )
        return df

    def export(self):
        data = self.read()
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
    SriLanka().export()


if __name__ == "__main__":
    main()
