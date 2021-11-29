from cowidev.utils.web import get_soup
from cowidev.utils.clean import clean_count, extract_clean_date
from cowidev.testing.utils.incremental import increment


class Bahrain:
    location: str = "Bahrain"
    units: str = "units unclear"
    source_label: str = "Ministry of Health"
    source_url: str = "https://www.moh.gov.bh/COVID19"
    testing_type: str = "PCR only"
    notes: str = ""

    def _parse_data(self):
        soup = get_soup(self.source_url)
        date_raw = soup.select_one("#lastupdated ul li").text
        return {
            "count": clean_count(soup.select_one("#renderbody table th span").text),
            "date": extract_clean_date(date_raw, regex=r"(\d+/\d+/20\d+).*", date_format="%d/%m/%Y"),
        }

    def export(self):
        data = self._parse_data()
        increment(
            count=data["count"],
            sheet_name=self.location,
            country=self.location,
            units=self.units,
            date=data["date"],
            source_url=self.source_url,
            source_label=self.source_label,
        )


def main():
    Bahrain().export()


if __name__ == "__main__":
    main()
