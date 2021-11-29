import pandas as pd
from datetime import date
import numpy as np


def main():
    print("Downloading Israel data…")
    url = "https://datadashboardapi.health.gov.il/api/queries/patientsPerDate"
    israel = pd.read_json(url)
    israel.loc[:, "date"] = pd.to_datetime(israel["date"])

    stock = israel[["date", "Counthospitalized", "CountCriticalStatus"]].copy()
    stock.loc[:, "date"] = stock["date"].dt.date
    stock.loc[stock["date"].astype(str) < "2020-08-17", "CountCriticalStatus"] = np.nan
    stock = stock.melt("date", var_name="indicator")

    flow = israel[["date", "new_hospitalized", "serious_critical_new"]].copy()
    flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    flow = flow[flow["date"] <= date.today()]
    flow = flow.groupby("date", as_index=False).agg(
        {"new_hospitalized": ["sum", "count"], "serious_critical_new": "sum"}
    )
    flow.columns = ["date", "new_hospitalized", "count", "serious_critical_new"]
    flow = flow[flow["count"] == 7]
    flow = flow.drop(columns="count").melt("date", var_name="indicator")

    israel = pd.concat([stock, flow]).dropna(subset=["value"])
    israel.loc[:, "indicator"] = israel["indicator"].replace(
        {
            "Counthospitalized": "Daily hospital occupancy",
            "CountCriticalStatus": "Daily ICU occupancy",
            "new_hospitalized": "Weekly new hospital admissions",
            "serious_critical_new": "Weekly new ICU admissions",
        }
    )

    israel.loc[:, "entity"] = "Israel"
    israel.loc[:, "iso_code"] = "ISR"
    israel.loc[:, "population"] = 9291000

    return israel
