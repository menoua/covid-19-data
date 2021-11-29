import pandas as pd

from cowidev.utils.web import request_json
from cowidev.utils import paths


class Zimbabwe:
    def __init__(self, source_url: str, location: str, columns_rename: dict = None):
        self.source_url = source_url
        self.location = location
        self.columns_rename = columns_rename

    def read(self) -> pd.DataFrame:
        url = "https://services9.arcgis.com/DnERH4rcjw7NU6lv/arcgis/rest/services/Vaccine_Distribution_Program/FeatureServer/2/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=date_reported%2Cfirst_doses%2Csecond_doses&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token="
        data = request_json(url)
        return pd.DataFrame.from_records(elem["attributes"] for elem in data["features"])

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.columns_rename:
            return df.rename(columns=self.columns_rename)
        return df

    def calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.fillna(0)
        df["total_vaccinations"] = df.people_vaccinated + df.people_fully_vaccinated
        df = df.groupby("date", as_index=False).sum().sort_values("date")
        df[["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]] = (
            df[["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]].cumsum().astype(int)
        )
        return df[df.total_vaccinations > 0]

    def format_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=pd.to_datetime(df.date, unit="ms").dt.date.astype(str))

    def enrich_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url,
        )

    def enrich_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str):
            if date < "2021-03-29":
                return "Sinopharm/Beijing"
            elif date < "2021-03-30":
                return "Oxford/AstraZeneca, Sinopharm/Beijing"
            elif date < "2021-06-11":
                return "Oxford/AstraZeneca, Sinopharm/Beijing, Sinovac"
            else:
                return "Oxford/AstraZeneca, Sinopharm/Beijing, Sinovac, Sputnik V"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.rename_columns)
            .pipe(self.format_date)
            .pipe(self.calculate_metrics)
            .pipe(self.enrich_columns)
            .pipe(self.enrich_vaccine)
        )

    def to_csv(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)


def main():
    Zimbabwe(
        source_url="https://www.arcgis.com/home/webmap/viewer.html?url=https://services9.arcgis.com/DnERH4rcjw7NU6lv/ArcGIS/rest/services/Vaccine_Distribution_Program/FeatureServer&source=sd",
        location="Zimbabwe",
        columns_rename={
            "date_reported": "date",
            "first_doses": "people_vaccinated",
            "second_doses": "people_fully_vaccinated",
        },
    ).to_csv()


if __name__ == "__main__":
    main()
