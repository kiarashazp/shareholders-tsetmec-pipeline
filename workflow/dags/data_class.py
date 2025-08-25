from dataclasses import dataclass


@dataclass
class SymbolData:
    ins_code: str


@dataclass
class HolderData:
    holder_code: str
    holder_name: str


@dataclass
class HoldingDailyData:
    symbol: SymbolData
    holder: HolderData
    trade_date: str
    shares: int
    percentage: float
