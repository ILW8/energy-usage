import pandas as pd
from pathlib import Path
import requests
import sys


BASE_URL = "https://api.octopus.energy"
PRODUCT_CODE = "AGILE-18-02-21"
TARIFF_CODE = f"E-1R-{PRODUCT_CODE}-C"
TARIFF_URL = f"{BASE_URL}/v1/products/{PRODUCT_CODE}/electricity-tariffs/{TARIFF_CODE}"
UNIT_RATE_URL = f"{TARIFF_URL}/standard-unit-rates/"


def convert_xlsx_to_csv():
    """
    data from energy reader is in .xlsx and I don't want to deal with pandas DFs
    :return: void
    """
    xlsx_path = 'Eve_Energy_1908_Total_Consumption.xlsx'
    csv_path = 'Eve_Energy_1908_Total_Consumption.csv'

    if not Path(xlsx_path).exists():
        print(f"missing input file '{xlsx_path}'")
        sys.exit(1)

    needs_updating = True
    if Path(csv_path).exists():
        input_mtime = Path(xlsx_path).stat().st_mtime
        csv_mtime = Path(csv_path).stat().st_mtime
        if csv_mtime > input_mtime:
            print(f"output is newer than input, skipping conversion ({csv_mtime} > {input_mtime})")
            needs_updating = False

    if needs_updating:
        df = pd.read_excel(xlsx_path)

        # truncate misc header data
        df[2:].to_csv(csv_path, index=False, header=False)


def fetch_agile_prices(period_from=None, period_to=None) -> list:
    """
    Fetches agile prices, follows pagination

    :param period_from: string in iso8601 format of start of data
    :param period_to: string in iso8601 format of end of data
    :return: dict containing rates, or an empty dict if failure
    """
    results = []

    query_params = {"page_size": 1500}
    if period_from is not None and isinstance(period_from, str):
        query_params["period_from"] = period_from
    if period_to is not None and isinstance(period_to, str):
        query_params["period_to"] = period_to

    req = requests.get(UNIT_RATE_URL, params=query_params)
    print(f"GET {req.url}")
    if req.status_code != 200:
        print(f"something went wrong, request returned with status {req.status_code}")
        return []
    response_json = req.json()
    results.extend(response_json['results'])

    while response_json['next'] is not None:
        req = requests.get(response_json['next'])
        print(f"GET {req.url}")
        if req.status_code != 200:
            print(f"GET {response_json['next']}: failed with status code {req.status_code}")
            break

        response_json = req.json()
        results.extend(response_json['results'])

    return results


if __name__ == '__main__':
    convert_xlsx_to_csv()
    prices: list = fetch_agile_prices(period_from="2024-01-01T00:00:00Z")
    prices_sorted: list = sorted(prices, key=lambda entry: entry['valid_from'])
    print()
