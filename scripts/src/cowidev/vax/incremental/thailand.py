import re
import requests
from datetime import datetime
import tempfile

from bs4 import BeautifulSoup
import pandas as pd
from pdfreader import SimplePDFViewer

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import clean_date, localdate
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import merge_with_current_data
from cowidev.utils import paths


class Thailand:
    location: str = "Thailand"
    source_url: str = "https://ddc.moph.go.th/dcd/pagecontent.php?page=643&dept=dcd"
    base_url_template: str = "https://ddc.moph.go.th/vaccine-covid19/diaryReportMonth/{}/9/2021"
    regex_date: str = r"\s?ข้อมูล ณ วันที่ (\d{1,2}) (.*) (\d{4})"
    _year_difference_conversion = 543
    _current_month = localdate("Asia/Bangkok", date_format="%m")

    @property
    def regex_vax(self):
        regex_aux = r"\((?:รา|รำ)ย\)"
        regex_vax = (
            r" ".join([f"เข็มที่ {i} {regex_aux}" for i in range(1, 4)])
            + r" รวม \(โดส\)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)"
        )
        return regex_vax

    def read(self, last_update: str) -> pd.DataFrame:
        # Get Newest Month Report Page
        url_month = self.base_url_template.format(self._current_month)
        soup_month = get_soup(url_month)
        # Get links
        df = self._parse_data(soup_month, last_update)
        return df

    def _parse_data(self, soup: BeautifulSoup, last_update: str):
        links = self._get_month_links(soup)
        records = []
        for link in links:
            # print(link["date"])
            if link["date"] <= last_update:
                break
            records.append(self._parse_metrics(link))
        return pd.DataFrame(records)

    def _get_month_links(self, soup):
        links = soup.find_all("a", class_="selectModelMedia")
        links = [
            {
                "link": link.get("href"),
                "date": self._parse_date_from_link_title(link.parent.parent.text.strip()),
            }
            for link in links
        ]
        return sorted(links, key=lambda x: x["date"], reverse=True)

    def _parse_date_from_link_title(self, title):
        match = re.search(r".*สรุปวัคซีน ประจำวันที่\s+(\d+) .* (25\d\d)", title).group(1, 2)
        year = int(match[1]) - self._year_difference_conversion
        return clean_date(f"{year}-{self._current_month}-{match[0]}", "%Y-%m-%d")

    def _parse_metrics(self, link: str):
        raw_text = self._text_from_pdf(link["link"])
        text = self._substitute_special_chars(raw_text)
        record = self._parse_variables(text)
        record["date"] = link["date"]
        record["source_url"] = link["link"].replace(" ", "%20")
        return record

    def _text_from_pdf(self, pdf_link: str):
        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, mode="wb") as f:
                f.write(requests.get(pdf_link).content)
            with open(tf.name, mode="rb") as f:
                viewer = SimplePDFViewer(f)
                viewer.render()
                raw_text = "".join(viewer.canvas.strings)
        return raw_text

    def _substitute_special_chars(self, raw_text: str):
        """Correct Thai Special Character Error."""
        special_char_replace = {
            "\uf701": "\u0e34",
            "\uf702": "\u0e35",
            "\uf703": "\u0e36",
            "\uf704": "\u0e37",
            "\uf705": "\u0e48",
            "\uf706": "\u0e49",
            "\uf70a": "\u0e48",
            "\uf70b": "\u0e49",
            "\uf70e": "\u0e4c",
            "\uf710": "\u0e31",
            "\uf712": "\u0e47",
            "\uf713": "\u0e48",
            "\uf714": "\u0e49",
        }
        special_char_replace = dict((re.escape(k), v) for k, v in special_char_replace.items())
        pattern = re.compile("|".join(special_char_replace.keys()))
        text = pattern.sub(lambda m: special_char_replace[re.escape(m.group(0))], raw_text)
        return text

    def _parse_variables(self, text: str):
        metrics = re.search(self.regex_vax, text).groups()
        people_vaccinated = clean_count(metrics[0])
        people_fully_vaccinated = clean_count(metrics[1])
        total_boosters = clean_count(metrics[2])
        total_vaccinations = clean_count(metrics[3])
        return {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "total_boosters": total_boosters,
        }

    def _parse_date(self, text: str):
        thai_date_replace = {
            # Months
            "มกราคม": 1,
            "กุมภาพันธ์": 2,
            "มีนาคม": 3,
            "เมษายน": 4,
            "พฤษภาคม": 5,
            "พฤษภำคม": 5,
            "มิถุนายน": 6,
            "มิถุนำยน": 6,
            "กรกฎาคม": 7,
            "กรกฎำคม": 7,
            "สิงหาคม": 8,
            "สิงหำคม": 8,
            "กันยายน": 9,
            "ตุลาคม": 10,
            "พฤศจิกายน": 11,
            "ธันวาคม": 12,
        }
        date_raw = re.search(self.regex_date, text)
        day = clean_count(date_raw.group(1))
        month = thai_date_replace[date_raw.group(2)]
        year = clean_count(date_raw.group(3)) - self._year_difference_conversion
        return clean_date(datetime(year, month, day))

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinovac")

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_location).pipe(self.pipe_vaccine)

    def to_csv(self):
        output_file = paths.out_vax(self.location)
        last_update = pd.read_csv(output_file).date.max()
        df = self.read(last_update)
        if not df.empty:
            df = df.pipe(self.pipeline)
            df = merge_with_current_data(df, output_file)
            df.to_csv(output_file, index=False)


def main():
    Thailand().to_csv()


if __name__ == "__main__":
    main()
