import os
import requests
import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm


# Constants
OWNER = "CSSEGISandData"
REPO = "COVID-19"
PATH = "csse_covid_19_data/csse_covid_19_daily_reports"
URL = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{PATH}"


def extract(url):
    download_urls = []
    response = requests.get(url)
    for data in tqdm(response.json(), desc="Extracting"):
        if data["name"].endswith(".csv"):
            download_urls.append(data["download_url"])

    return download_urls


relabel = {
    "Province/State": "Province_State",
    "Country/Region": "Country_Region",
    "Last Update": "Last_Update",
    "Latitude": "Lat",
    "Longitude": "Long_",
}


def transform(dat, filename):
    for label in dat:
        if label in relabel:
            dat = dat.rename(columns={label: relabel[label]})

    labels = [
        "Province/State",
        "Country/Region",
        "Last Update",
        "Confirmed",
        "Deaths",
        "Recovered",
    ]

    if "Last_Update" not in dat:
        dat["Last_Update"] = pd.to_datetime(filename)

    for label in labels:
        if label not in dat:
            dat[label] = np.nan

    return dat[labels]


def load(filenames, db_name, debug=False):
    conn = sqlite3.connect(f"{db_name}.db")

    if debug:
        for i, file_path in tqdm(list(enumerate(filenames)), desc="Uploading"):
            dat = pd.read_csv(file_path)

            file_name = os.path.basename(file_path).split(".")[0]
            transform(dat, file_name)

            if i == 0:
                dat.to_sql(db_name, con=conn, index=False, if_exists="replace")
            else:
                dat.to_sql(db_name, con=conn, index=False, if_exists="append")


db_name = "example"
download_urls = extract(URL)
load(download_urls, db_name, debug=True)
