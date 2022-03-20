import json
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, JSON, Date


Base = declarative_base()


class MarketPrice(Base):
    __tablename__ = 'market_price'
    # id = Column(String, primary_key=True)
    symbol = Column(String, primary_key=True)
    price = Column(Integer)
    date = Column(String,primary_key=True)
    
class ExchangeRate(Base):
    __tablename__ = 'exchange_rate'
    exchange_rates = Column(String)
    date = Column(String, primary_key=True)
