import pandas as pd
from pathlib import Path
import sys


if __name__ == '__main__':
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
