from pytest import Mark
import load_data, read_data
from datetime import datetime
from extensions import session
from model import MarketPrice

# Standard inputs
symbol ="AAPL"
currency = "INR"
start = "2022-03-09"
end = "2022-03-10"
start_date= datetime.strptime(start, "%Y-%m-%d")
end_date=datetime.strptime(end, "%Y-%m-%d")

def test_wrong_stock_symbol_should_throw_error():
    get_data_return = load_data.get_data("test",currency,start_date,end_date)
    assert get_data_return["status"] == "fail"
    
def test_exchange_rate():
    start_date_exchange = start_date.strftime("%Y-%m-%d")
    exchange_rate_return = load_data.get_exchange_rate(start_date_exchange, currency)
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

def test_stock_price_should_persist_in_database_after_fetching():
    get_market_price_data = read_data.fetch_market_data(symbol, start_date, end_date)
    assert len(get_market_price_data)>0
    
def test_integration_test_data_should_persist_in_database():
    check_data_for_dates = read_data.fetch_market_data(symbol, start_date, end_date)
    if(len(check_data_for_dates) >0):
        session.query(MarketPrice).filter(MarketPrice.symbol == symbol, MarketPrice.date>=start_date, MarketPrice.date<=end_date).delete(synchronize_session=False)
        session.commit()
    load_data.get_data(symbol, currency, start_date, end_date)
    check_data_for_dates_after_running_scipt = read_data.fetch_market_data(symbol, start_date, end_date)
    assert len(check_data_for_dates_after_running_scipt) > 0
    

    

        
    
        