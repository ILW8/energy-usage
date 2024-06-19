import csv
import datetime
from typing import Union
from tqdm import tqdm
import pandas as pd
from pathlib import Path
import requests
import sys
from intervaltree import IntervalTree
# from matplotlib import pyplot


XLSX_PATH = 'Eve_Energy_1908_Total_Consumption.xlsx'
CSV_PATH = 'Eve_Energy_1908_Total_Consumption.csv'

BASE_URL = "https://api.octopus.energy"
PRODUCT_CODE = "AGILE-18-02-21"
TARIFF_CODE = f"E-1R-{PRODUCT_CODE}-C"
TARIFF_URL = f"{BASE_URL}/v1/products/{PRODUCT_CODE}/electricity-tariffs/{TARIFF_CODE}"
UNIT_RATE_URL = f"{TARIFF_URL}/standard-unit-rates/"

STANDING_CHARGE_PER_DAY = 0.3479

# 567.896/30/24*1000, 567.896 was the total amount of energy used in 28th apr - 27th may
MISSING_DATA_FILLER_WATTAGE = 788.744

# can't be bothered to do input handing
START_TIME_UNIX = 1714262400.0
END_TIME_UNIX = 1716854400.0
# START_TIME_UNIX = 1709078400.0
# END_TIME_UNIX = 1714262400.0


def convert_xlsx_to_csv():
    """
    data from energy reader is in .xlsx and I don't want to deal with pandas DFs
    :return: void
    """
    if not Path(XLSX_PATH).exists():
        print(f"missing input file '{XLSX_PATH}'")
        sys.exit(1)

    needs_updating = True
    if Path(CSV_PATH).exists():
        input_mtime = Path(XLSX_PATH).stat().st_mtime
        csv_mtime = Path(CSV_PATH).stat().st_mtime
        if csv_mtime > input_mtime:
            print(f"output is newer than input, skipping conversion ({csv_mtime} > {input_mtime})")
            needs_updating = False

    if needs_updating:
        df = pd.read_excel(XLSX_PATH, header=3)

        # truncate misc header data
        df.sort_values(by=["Date"]).to_csv(CSV_PATH, index=False, header=False)


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
    with open(CSV_PATH, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # skip header

        tree = IntervalTree()

        # we want to get a wattage for a time range
        last_dt: Union[datetime, None] = None
        for line in tqdm(csv_reader):
            dt = datetime.datetime.strptime(line[0], "%Y-%m-%d %H:%M:%S")

            if last_dt is not None:
                time_elapsed: datetime.timedelta = dt - last_dt
                energy_consumed = float(line[1])
                wattage = energy_consumed * 3600 / time_elapsed.total_seconds()

                if time_elapsed > datetime.timedelta(minutes=15):  # we expect data every 10 minutes
                    wattage = MISSING_DATA_FILLER_WATTAGE

                interval_begin = int(last_dt.timestamp())
                interval_end = interval_begin + int(time_elapsed.total_seconds())
                if START_TIME_UNIX <= interval_end and interval_begin <= END_TIME_UNIX:
                    tree.addi(interval_begin, interval_end, wattage)

            last_dt = dt

    # pyplot.step([item.begin for item in sorted(tree.items(), key=lambda x: x.begin)],
    #             [item.data for item in sorted(tree.items(), key=lambda x: x.begin)])
    # pyplot.show()

    prices_sorted: list = sorted(
        fetch_agile_prices(period_from=f"{datetime.datetime.fromtimestamp(START_TIME_UNIX).isoformat()}Z",
                           period_to=f"{datetime.datetime.fromtimestamp(END_TIME_UNIX).isoformat()}Z"),
        key=lambda entry: entry['valid_from']
    )

    # list of tuples: (start timestamp, usage in Wh, usage cost in GBP)
    usage_by_price_incl_vat = []
    usage_by_price_excl_vat = []
    for price in prices_sorted:
        price_from = int(datetime.datetime.fromisoformat(price['valid_from']).timestamp())
        price_to = int(datetime.datetime.fromisoformat(price['valid_to']).timestamp())
        price_incl_vat = price['value_inc_vat']
        price_excl_vat = price['value_exc_vat']

        usage_segments = tree[price_from:price_to]
        for usage_segment in usage_segments:
            # find overlap between usage segment and price valid range
            overlap_begin = max(usage_segment.begin, price_from)
            overlap_end = min(usage_segment.end, price_to)
            segment_duration_seconds = overlap_end - overlap_begin

            usage_kwh = usage_segment.data * segment_duration_seconds / 3_600_000
            usage_cost = price_incl_vat * usage_kwh / 100.
            usage_cost_excl_vat = price_excl_vat * usage_kwh / 100.
            usage_by_price_incl_vat.append((overlap_begin, usage_kwh, usage_cost))
            usage_by_price_excl_vat.append((overlap_begin, usage_kwh, usage_cost_excl_vat))

    total_usage_kwh = sum([kwh for _, kwh, _ in usage_by_price_incl_vat])
    total_usage_cost_incl_vat = sum([cost for _, _, cost in usage_by_price_incl_vat])
    total_usage_cost_excl_vat = sum([cost for _, _, cost in usage_by_price_excl_vat])
    total_standing_charge = (END_TIME_UNIX - START_TIME_UNIX)/86_400 * STANDING_CHARGE_PER_DAY
    print(f"{total_usage_kwh=:.3f}kWh, {total_usage_cost_incl_vat=:.3f}GBP, "
          f"{total_usage_cost_excl_vat=:.3f}, {total_standing_charge=:.3f}GBP")
