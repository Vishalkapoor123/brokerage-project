from json import load
from unicodedata import name

from pandas import DataFrame
import load_data, read_data
from datetime import datetime

# Standard inputs
symbol ="AAPL"
currency = "INR"
start = "2022-03-10"
end = "2022-03-20"
start_date= datetime.strptime(start, "%Y-%m-%d")
end_date=datetime.strptime(end, "%Y-%m-%d")

def test_wrong_stock_symbol_should_throw_error():
    get_data_return = load_data.get_data("test",currency,start_date,end_date)
    assert get_data_return["status"] == "fail"
    
def test_exchange_rate():
    exchange_rate_return = load_data.get_exchange_rate(start_date, currency)
    assert exchange_rate_return > 0
    
def test_fetch_exchange_rate_should_return_list():
    fetch_exchange_rates_return = read_data.fetch_exchange_rates(start_date)
    assert type(fetch_exchange_rates_return) == list
    
def test_fetch_market_data_should_return_list():
    fetch_market_data_return = read_data.fetch_market_data(symbol, start_date, end_date)
    assert type(fetch_market_data_return) == list 
    
def test_get_data_should_return_success_status():
    get_data_return = load_data.get_data(symbol, currency, start_date, end_date)
    assert get_data_return["status"] == "success"
        