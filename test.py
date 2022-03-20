from json import load
from unicodedata import name
import load_data
from datetime import datetime

def test_wrong_stock_symbol_should_throw_error():
    symbol ="test"
    currency = "INR"
    start = "2022-03-10"
    end = "2022-03-20"
    start_date= datetime.strptime(start, "%Y-%m-%d")
    end_date=datetime.strptime(end, "%Y-%m-%d")
    get_data_return = load_data.get_data(symbol,currency,start_date,end_date)
    assert get_data_return["status"] == "fail"
    
def test_exchange_rate():
    exchange_rate_return = load_data.get_exchange_rate(datetime.today(), "INR")
    assert exchange_rate_return > 0
        