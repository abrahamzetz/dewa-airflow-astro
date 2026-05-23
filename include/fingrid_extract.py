"""
Fetch one day of Fingrid data and return rows ready for Snowflake.

Same shape as batch/fingrid.py but wrapped in a function so the DAG can call it.
No Airflow imports - runnable standalone for debugging:

    cd airflow
    FINGRID_API_KEY=xxx python include/fingrid_extract.py
"""

import datetime
import os
import requests

DATASET_ID = 358  # Electricity consumption in Finland, hourly data


def fetch_consumption(day, api_key):
    next_day = day + datetime.timedelta(days=1)

    url = f"https://data.fingrid.fi/api/datasets/{DATASET_ID}/data"
    params = {
        "startTime": f"{day}T00:00:00Z",
        "endTime":   f"{next_day}T00:00:00Z",
        "pageSize": 20000,
    }

    response = requests.get(url, headers={"x-api-key": api_key}, params=params)
    data = response.json()

    return [
        (DATASET_ID, r["startTime"], r["endTime"], r["value"])
        for r in data["data"]
    ]


if __name__ == "__main__":
    # Fingrid publishes with a delay, so fetch a week ago for a reliable test.
    day = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=7)
    rows = fetch_consumption(day, os.environ["FINGRID_API_KEY"])
    print(f"Fetched {len(rows)} rows for {day}")
    print(rows[:3])
