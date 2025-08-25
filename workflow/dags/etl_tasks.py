import csv
import json

from dataclasses import is_dataclass
from datetime import datetime, timedelta
from itertools import product

from airflow.decorators import task
import requests
from persiantools.jdatetime import JalaliDate
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv, find_dotenv

from data_class import *
from database import *
from models import *


load_dotenv(find_dotenv())
shared_directory = os.getenv('SHARED_DIR', "/opt/airflow/shared")
prefix = os.getenv('prefix_csv_shareholders', "shareholders")


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


@task
def fetch_shareholders(symbol: SymbolData, date_str: str):
    """
        Fetch shareholder information for a given symbol and date from the TSETMC API.

        This function sends a request to the TSETMC Shareholder API and retrieves
        shareholder data for the specified symbol and date. It validates the date field
        in each record and creates structured objects for valid entries.

        Args:
            symbol (SymbolData): A SymbolData object representing the target symbol.
            date_str (str): Trade date in "YYYYMMDD" format.
        Returns:
            list[HoldingDailyData]: A list of HoldingDailyData objects containing:
                - symbol: The given symbol
                - holder: A HolderData object with shareholder name and code
                - trade_date: The date of the data
                - shares: Number of shares held
                - percentage: Percentage of ownership
        Raises:
            RequestException and ValueError
    """
    url = f"https://cdn.tsetmc.com/api/Shareholder/{symbol}/{date_str}"
    response = requests.get(url)
    if not str(response.status_code).startswith("2"):
        return []

    daily_information_holders = response.json().get("shareShareholder", [])
    shareholders = []
    for daily_information_holder in daily_information_holders:
        if str(daily_information_holder["dEven"]) != date_str:
            continue

        shareholder = HolderData(
            holder_name=daily_information_holder.get("shareHolderName"),
            holder_code=str(daily_information_holder.get("shareHolderShareID")),
        )
        holding_daily_data = HoldingDailyData(
            symbol=symbol,
            holder=shareholder,
            trade_date=date_str,
            shares=daily_information_holder.get("numberOfShares"),
            percentage=daily_information_holder.get("perOfShares"),
        )
        shareholders.append(holding_daily_data)

    return shareholders


@task
def save_to_csv(records: list):
    """
        Save a list of HoldingDailyData records to a CSV file.
        Each HoldingDailyData object is flattened into a dictionary with the following fields:
            - symbol (str): Instrument code (from SymbolData.ins_code)
            - holder_code (str): Unique code of the shareholder (from HolderData.holder_code)
            - holder_name (str): Display name of the shareholder (from HolderData.holder_name)
            - trade_date (str): Trade date, typically in "YYYYMMDD" format
            - shares (int|float): Number of shares held
            - percentage (float): Percentage of ownership
        Args:
            records (list): List of HoldingDailyData objects to be written.
        Returns:
            str: The absolute path to the generated CSV file.
        Raises:
            ValueError: If a record cannot be flattened to required fields.
            OSError: If directory creation or file writing fails.
    """
    def _flatten(row):
        """
            Convert a HoldingDailyData record to a flat dict matching `fieldnames`.
        """
        if is_dataclass(row):
            symbol_obj = getattr(row, "symbol", None)
            holder_obj = getattr(row, "holder", None)

            symbol_code = getattr(symbol_obj, "ins_code", None) if symbol_obj else None
            holder_code = getattr(holder_obj, "holder_code", None) if holder_obj else None
            holder_name = getattr(holder_obj, "holder_name", None) if holder_obj else None
            trade_date = getattr(row, "trade_date", None)
            shares = getattr(row, "shares", None)
            percentage = getattr(row, "percentage", None)

        else:
            raise ValueError(f"Unsupported record type: {type(row)}")

        data = {
            "symbol": str(symbol_code) if symbol_code else "",
            "holder_code": str(holder_code) if holder_code else "",
            "holder_name": str(holder_name) if holder_name else "",
            "trade_date": str(trade_date) if trade_date else "",
            "shares": shares if shares else "",
            "percentage": percentage if percentage else "",
        }

        if not (data["symbol"] and data["trade_date"] and data["holder_name"]):
            raise ValueError(f"Record missing required fields (symbol/trade_date/holder_name): {row}")
        return data

    output_path = f"{shared_directory}/{prefix}_{datetime.now().timestamp()}.csv"
    if not records:
        return output_path

    fieldnames = ["symbol", "holder_code", "holder_name", "trade_date", "shares", "percentage"]
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        if records:
            for record in records:
                writer.writerow(_flatten(row=record))

    return output_path


@task
def upsert_data_to_postgres(csv_path: str, session_factory=SessionLocal):
    """
        Load shareholder holdings data from a CSV file into the PostgreSQL database.

        Args:
            csv_path (str): Path to the input CSV file.
            session_factory (Callable): A SQLAlchemy session factory, defaults to `SessionLocal`.
        Returns:
            bool: True if at least one new `HoldingDaily` record was inserted,
                  False if no new records were added.
        Raises:
            FileNotFoundError, ValueError, SQLAlchemyError
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    required_headers = {"symbol", "holder_code", "holder_name", "trade_date", "shares", "percentage"}
    inserted_any = False

    with session_factory() as session:
        try:
            with open(csv_path, "r", newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                headers = set(reader.fieldnames or [])
                missing = required_headers - headers
                if missing:
                    raise ValueError(f"CSV missing required columns: {', '.join(sorted(missing))}")

                with session.begin():
                    for row in reader:
                        symbol_code = (row["symbol"] or "").strip()
                        holder_code = (row["holder_code"] or "").strip()
                        holder_name = (row["holder_name"] or "").strip()
                        trade_date_str = (row["trade_date"] or "").strip()

                        if not (symbol_code and holder_code and holder_name and trade_date_str):
                            continue

                        try:
                            trade_date = datetime.strptime(trade_date_str, "%Y%m%d").date()
                        except ValueError:
                            raise ValueError(f"Invalid trade_date '{trade_date_str}' (expected YYYYMMDD).")

                        try:
                            shares = str(row.get('shares'))
                            percentage = str(row.get("percentage"))
                            shares = int(float(shares.strip().replace(",", ""))) if shares else 0
                            percentage = float(percentage.strip().replace(",", "")) if percentage else 0.0
                        except (ValueError, TypeError, AttributeError):
                            shares, percentage = 0, 0.0

                        # get_or_create Symbol
                        symbol_obj = session.execute(select(Symbol).where(Symbol.ins_code == symbol_code)).scalar_one_or_none()
                        if symbol_obj is None:
                            symbol_obj = Symbol(ins_code=symbol_code)
                            session.add(symbol_obj)
                            session.flush()

                        # get_or_create Holder
                        holder_obj = session.execute(
                            select(Holder).where(Holder.holder_code == holder_code)
                        ).scalar_one_or_none()
                        if holder_obj is None:
                            holder_obj = Holder(holder_code=holder_code, holder_name=holder_name)
                            session.add(holder_obj)
                            session.flush()

                        # is_exists for (symbol, holder, date) in HoldingDaily
                        exists = session.execute(
                            select(HoldingDaily.id).where(
                                HoldingDaily.symbol_id == symbol_obj.id,
                                HoldingDaily.holder_id == holder_obj.id,
                                HoldingDaily.trade_date == trade_date,
                            )
                        ).scalar_one_or_none()

                        if exists is None:
                            session.add(
                                HoldingDaily(
                                    symbol_id=symbol_obj.id, holder_id=holder_obj.id, trade_date=trade_date,
                                    shares=shares, percentage=percentage,
                                )
                            )
                            inserted_any = True
            return inserted_any

        except SQLAlchemyError:
            session.rollback()
            raise


@task
def cleanup(successfully, directory=shared_directory, prefix_file="shareholders"):
    if not successfully:
        return False

    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist.")
        return False

    for filename in os.listdir(directory):
        if filename.startswith(f"{prefix_file}_") and filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            try:
                os.remove(file_path)
                print(f"Deleted file: {file_path}")
                return True
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")
                return False
