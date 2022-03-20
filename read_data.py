import logging
from model import ExchangeRate, MarketPrice
from extensions import session


logging.getLogger().setLevel(logging.INFO)

# read exchange_rate database
def fetch_exchange_rates(date):
    logging.info("Fetching rates from database for date: {}".format(date))
    try:
        exchange_rate_data = session.query(ExchangeRate.exchange_rates, ExchangeRate.date).filter(ExchangeRate.date == date.strftime("%Y-%m-%d")).all()
        return exchange_rate_data
    except:
        return []

# read market_price database
def fetch_market_data(symbol, start_date, end_date):
    logging.info("Fetching market price from database for stock symbol: {}".format(symbol))
    try:
        stock_price_data = session.query(MarketPrice.symbol, MarketPrice.price, MarketPrice.date).filter(MarketPrice.symbol == symbol, MarketPrice.date >=start_date, MarketPrice.date <= end_date).all()
        return stock_price_data
    except:
        return []

