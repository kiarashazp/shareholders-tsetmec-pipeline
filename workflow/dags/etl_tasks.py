import csv
import json
import os
from datetime import timedelta
from itertools import product

from airflow.decorators import task
import requests
from persiantools.jdatetime import JalaliDate

from data_class import *


@task
def read_symbols_from_file(file_path: str):
    """
        Read symbol data from a JSON or CSV file and return a list of SymbolData objects.
        This function supports two file formats:
          - JSON: The file must contain a key named "ins_codes" with a list of symbols.
          - CSV:  The file must contain a column named "ins_codes" with symbol values.
        Args:
            file_path (str): Path to the input file (must be .json or .csv).
        Returns:
            list[SymbolData]: A list of SymbolData objects extracted from the file.
        Raises:
            FileNotFoundError: If the given file path does not exist.
            KeyError: If the CSV file does not contain an "ins_codes" column.
            ValueError: If the file format is not supported (only JSON and CSV allowed).
    """

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    _, extension = os.path.splitext(file_path)
    extension = extension.lower()

    if extension == '.json':
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        symbols = data.get("ins_codes", [])
        return [SymbolData(str(symbol)) for symbol in symbols if symbol]

    elif extension == '.csv':
        with open(file_path, "r", newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if "ins_codes" not in reader.fieldnames:
                raise KeyError("CSV file must contain 'ins_codes' column.")
            return [SymbolData(str(row["ins_codes"])) for row in reader if row.get("ins_codes")]

    else:
        raise ValueError("Unsupported file format. Use JSON or CSV.")


@task
def generate_dates():
    """
        Generate the most recent 10 valid working days based on the Jalali calendar and return them in Gregorian format

        The function checks each day starting from today and moves backward:
          - Skips weekends (Thursday and Friday in Jalali calendar).
          - Calls an external API (https://pnldev.com/api/calender) to determine  if the day is a holiday.
          - Collects only non-holiday working days.

        Once 10 valid working days are collected, they are converted to Gregorian dates and formatted as strings in the form "YYYYMMDD".

        Returns:
            list[str]: A list of 10 date strings representing recent working days
            in "YYYYMMDD" format.

        Raises:
            RequestException and KeyError
    """
    base_url = "https://pnldev.com/api/calender"
    current_date = JalaliDate.today()
    working_days = []
    while len(working_days) < 10:
        if current_date.weekday() in [5, 6]:
            current_date -= timedelta(days=1)
            continue

        url = f"{base_url}?year={current_date.year}&month={current_date.month}&day={current_date.day}"
        response = requests.get(url)
        if not str(response.status_code).startswith('2'):
            print(current_date, url, response.status_code, response)
            current_date -= timedelta(days=1)

        data = response.json()['result']
        if data.get('holiday', False):
            current_date -= timedelta(days=1)
            continue

        working_days.append(current_date)
        current_date -= timedelta(days=1)

    working_days = [day.to_gregorian() for day in working_days]
    transform_days = [str(date_time.isoformat()).replace('-', '') for date_time in working_days]
    return transform_days


@task
def make_combinations(symbols, dates):
    """
        Generate all possible combinations of symbols and dates.
        For each symbol in the given list and each date in the provided list, create a dictionary containing:
          - "symbol": the `ins_code` attribute of the symbol object
          - "date_str": the date string

        Args:
            symbols (list[SymbolData]): A list of SymbolData objects containing `ins_code`.
            dates (list[str]): A list of date strings (e.g., "YYYYMMDD").

        Returns:
            list[dict]: A list of dictionaries, each representing a (symbol, date) combination.
            Example: [
                {"symbol": "ABC123", "date_str": "20250101"},
                {"symbol": "XYZ456", "date_str": "20250101"},
                ...
            ]
    """
    return [
        {"symbol": combination[0].ins_code, "date_str": combination[1]} for combination in list(product(symbols, dates))
    ]


