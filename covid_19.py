import os
import requests
import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm


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
    dat.rename(columns=relabel, inplace=True)

    if "Last_Update" not in dat.columns:
        dat["Last_Update"] = pd.to_datetime(filename)

    required_columns = [
        "Province/State",
        "Country/Region",
        "Lat",
        "Long_",
        "Last_Update",
        "Confirmed",
        "Deaths",
        "Recovered",
        "Active",
        "FIPS",
        "Admin2",
        "Combined_Key",
        "Incident_Rate",
        "Case_Fatality_Ratio",
    ]

    for col in required_columns:
        if col not in dat.columns:
            dat[col] = np.nan

    return dat


def load(filenames, db_name, debug=False):
    conn = sqlite3.connect(f"{db_name}.db")
    cursor = conn.cursor()

    if debug:
        for i, file_path in tqdm(
            list(enumerate(filenames)),
            desc="Uploading",
            ascii="-\|/-\|/-\|/-\|/-\|/-\|/-\#",
        ):
            dat = pd.read_csv(file_path)

            file_name = os.path.basename(file_path).split(".")[0]
            dat = transform(dat, file_name)

            if i == 0:
                dat.to_sql(db_name, con=conn, index=False, if_exists="replace")
            else:
                existing_columns = [
                    col[1] for col in cursor.execute(f"PRAGMA table_info({db_name})")
                ]
                new_columns = set(dat.columns) - set(existing_columns)

                for col in new_columns:
                    cursor.execute(f"ALTER TABLE {db_name} ADD COLUMN {col}")

                dat.to_sql(db_name, con=conn, index=False, if_exists="append")

    conn.commit()
    conn.close()
