from sqlalchemy import Column, String, BigInteger, Float, Date, ForeignKey, Integer, Index
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Symbol(Base):
    __tablename__ = "symbols"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ins_code = Column(String, unique=True, nullable=False, index=True)
    holdings = relationship("HoldingDaily", back_populates="symbol")


class Holder(Base):
    __tablename__ = "holders"
    id = Column(Integer, primary_key=True, autoincrement=True)
    holder_code = Column(String, unique=True, nullable=False, index=True)
    holder_name = Column(String, nullable=False)
    holdings = relationship("HoldingDaily", back_populates="holder")


class HoldingDaily(Base):
    __tablename__ = "holdings_daily"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol_id = Column(ForeignKey("symbols.id"), nullable=False)
    holder_id = Column(ForeignKey("holders.id"), nullable=False)
    trade_date = Column(Date, nullable=False)
    shares = Column(BigInteger, nullable=False)
    percentage = Column(Float, nullable=False)
    symbol = relationship("Symbol", back_populates="holdings")
    holder = relationship("Holder", back_populates="holdings")


Index("idx_holdings_symbol_date", HoldingDaily.symbol_id, HoldingDaily.trade_date)
Index("idx_holdings_holder_date", HoldingDaily.holder_id, HoldingDaily.trade_date)
Index("idx_holdings_date", HoldingDaily.trade_date)
