import json
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


Base = declarative_base()


class MarketPrice(Base):
    __tablename__ = 'market_price'
    symbol = Column(String, primary_key=True)
    price = Column(Integer)
    date = Column(String,primary_key=True)
    currency = Column(String)
    
class ExchangeRate(Base):
    __tablename__ = 'exchange_rate'
    base_currency = Column(String)
    exchange_rates = Column(String)
    date = Column(String, primary_key=True)
