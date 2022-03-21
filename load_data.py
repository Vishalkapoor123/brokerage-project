from datetime import datetime
import logging
from extensions import engine
import requests
import pandas as pd
import json
import argparse
from time import sleep
from read_data import fetch_exchange_rates, fetch_market_data

# logging initialization
logging.getLogger().setLevel(logging.INFO)

# Standard inputs
access_key_market = "019e2a6c1de5a911695d119972b224cd"
access_key_exchange = "353c659b37318718363726fa29e05532"
base_currency_market = "USD"
url_exchange_rates = "http://api.exchangeratesapi.io/v1"
url_market_price = "http://api.marketstack.com/v1"
stock_price_feature = "eod" #closing price


# Get exchange rate for a particular date
def get_exchange_rate(date, currency):
    
    #Create a placeholder dataframe with datetime index
    df_exchange_rate = pd.DataFrame(index = [date])
    df_exchange_rate.index.name = "date"
    
    #Fetch details from the Database
    data = fetch_exchange_rates(date)
    
    #If not found hit the API and fetch from the servers
    if(len(data) == 0):
        logging.info("Fetching exchange rates from server for date: {}".format(date))
        try:
            exchange_rate_url = "{base_url}/{date}?access_key={access_key}".format(base_url = url_exchange_rates, date = date, access_key = access_key_exchange)
            res = requests.get(exchange_rate_url)
            response_json = res.json()
            if(res.status_code == 200):
                exchange_rates = response_json["rates"]
                df_exchange_rate.insert(0, "exchange_rates", json.dumps(exchange_rates))
                df_exchange_rate.insert(1, "base_currency", response_json["base"])
                
                # Save exchange rates into exchange table
                df_exchange_rate.to_sql("exchange_rate", engine, if_exists = 'append')
                
                conversion_ratio = exchange_rates[currency]
            return conversion_ratio/exchange_rates[base_currency_market]
        except:
            logging.error("Server not reachable")
            return -1
        finally:
            #sleep for half second due to API limitations
            sleep(0.5)
    else:
        # Take from database and return conversion ratio
        exchange_rates = json.loads(data[0][0])
        return exchange_rates[currency]/exchange_rates[base_currency_market]
    
    
# Dataframe currency conversion  
def return_data(dataframe, currency):
    # If required currency is not same as base currency then convert else do nothing
    if(currency != base_currency_market):
        #iterate through dataframe
        for i, row in dataframe.iterrows():
            ex_rate = get_exchange_rate(i, currency)
            if(ex_rate > 0):
                # Change price to converted price
                converted_price =  round(float(row["price"])*float(ex_rate), 2)
                dataframe.at[i, 'price'] = converted_price
                dataframe.at[i,"currency"] = currency
            else:
                logging.error("Internal service error")
                return None
            
    logging.info("Results: \n {} \n".format(dataframe))
    return {"status":"success"}
        

# Main method
def get_data(symbol, currency, start_date, end_date):
    logging.info("Getting details...")
    
    #create a placeholder dataframe with datetimeindex
    timeseries = pd.date_range(start = start_date, end = end_date)
    df_market = pd.DataFrame(index = timeseries.strftime("%Y-%m-%d"))
    df_market.index.name = "date"
    df_market.insert(0, column = "price", value = None)
    
    # creating database and server copies of dataframe 
    df_market_database = df_market.copy()
    df_market_server = df_market.copy()
    
    #fetch data from database first and check if requirement can be satisfied
    data = fetch_market_data(symbol, start_date.strftime("%Y-%m-%d"), end_date)
    for i in range(len(data)):
        df_market_database.loc[data[i][2]] = data[i][1]
    df_market_database = df_market_database.dropna()
    df_size = df_market_database.size
    df_market_database.insert(1, "symbol", symbol)
    
    # If dataframe size is less than days delta then fetch details from the server
    if(df_size < (end_date-start_date).days+1):
        logging.info("Fetching details from the server...")
        try:
            offset = 0
            limit = 1000
            total_data = []
            url = "{base_url}/{feature}?access_key={access_key}&symbols={symbol}&date_from={start}&date_to={end}&limit={limit}".format(base_url = url_market_price, feature = stock_price_feature, access_key = access_key_market, symbol = symbol, start = start_date, end = end_date, limit = limit)
            res = requests.get(url)
            resp = res.json()
            total_data = res.json()["data"]
            
            #Handling large data cases when data is more than API limit
            while(resp["pagination"]["total"] > resp["pagination"]["limit"] + resp["pagination"]["offset"]):
                offset += limit
                url = "{base_url}/{feature}?access_key={access_key}&symbols={symbol}&date_from={start}&date_to={end}&limit={limit}&offset={offset}".format(base_url = url_market_price, feature = stock_price_feature, access_key = access_key_market, symbol = symbol, start = start_date, end = end_date, limit = limit, offset = offset)
                res = requests.get(url)
                resp = res.json()
                total_data += resp["data"]
            logging.info("Processing your request") 
            if(res.status_code == 200):
                count = len(total_data)
                for i in range(count):
                    temp = total_data[i]
                    temp["date"] = datetime.strptime(temp["date"], "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d") # date conversion into YYYY-MM-DD format
                    df_market_server.loc[temp["date"]] = temp["close"]
                df_market_server = df_market_server.replace([None], -1)
                df_market_server.insert(1, "symbol", symbol)
                df_market_server.insert(2, "currency", base_currency_market)
                
                # Take differences from database and server and persist new items that into database 
                df_diff = pd.concat([df_market_database, df_market_server])
                df_diff = df_diff[~df_diff.index.duplicated(keep = False)]
                # Store data into the database for market_price
                df_diff.to_sql("market_price", engine, if_exists = 'append')
                df_market_server = df_market_server.loc[df_market_server["price"] >= 0]
                return return_data(df_market_server, currency)
            else:
                if(res.status_code == 500):
                    logging.error("Internal server error, sorry for the inconvinience")
                    # return {"status":500, "info":"server error"}
                    return {"status":"fail"}
                elif(res.status_code == 422):
                    logging.error("Invalid symbol, {} is not a valid stock symbol. Please check again".format(symbol))
                    # return {"status":404, "info":"invalid symbol code"}
                    return {"status":"fail"}
                else:
                    logging.error(resp.error)
                    return {"status":"fail"}
        except:
            logging.error("Server not reachable")
            return {"status":"fail"}
    else:
        # Data is present in the database hence use that data
        df_market_database.insert(2, "currency", base_currency_market)
        df_market_database = df_market_database.loc[df_market_database["price"] >= 0]
        return return_data(df_market_database, currency)
   
            
def input_and_validate():
    logging.info("Verifying your inputs...")
    
    ############# commnd line argumanets ####################
    parser = argparse.ArgumentParser(description = 'Market Data')
    parser.add_argument("--symbol", type = str)
    parser.add_argument("--currency", type = str)
    parser.add_argument("--start", type = str)
    parser.add_argument("--end", type = str)
    args = parser.parse_args()
    
    symbol = args.symbol
    currency = args.currency
    start = args.start
    end = args.end
    
    #########################################################
    #Input validation
    if(symbol is None or len(symbol.strip()) == 0):
        logging.error("Stock symbol is required, please enter correct symbol of stock")
        return {"status":"failed"}
    
    if(currency is None or len(currency.strip()) == 0):
        logging.error("Currency is required, please enter correct symbol of currency")
        return {"status":"failed"}

    try:
        start_date = datetime.strptime(start, "%Y-%m-%d")
    except:
        logging.error("Start is required and it should be in format of YYYY-MM-DD")
        return {"status":"failed"}
    try: 
        end_date = datetime.strptime(end, "%Y-%m-%d")
    except:
        logging.error("End is required and it should be in format of YYYY-MM-DD")
        return {"status":"failed"}
    
    #Date validation
    if(start_date > end_date):
        logging.error("Invalid dates, start date cannot be greater than end date")
        return {"status":"failed"}
    else:
        #Get data from databases/APIs
        return get_data(symbol, currency, start_date, end_date)
            

# Main method
if __name__ == '__main__':
    input_and_validate()
    
