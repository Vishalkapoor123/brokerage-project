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
access_key_market = "68c5706c455f39f717a8bd04a1b2e8f0"
access_key_exchange = "353c659b37318718363726fa29e05532"
base_currency_exchange = "EUR"
base_currency_market = "USD"
url_exchange_rates = "http://api.exchangeratesapi.io/v1"
url_market_price = "http://api.marketstack.com/v1"
stock_price_feature = "eod" #closing price


# Get exchange rate by dates
def get_exchange_rate(date, currency):
    #Create a placeholder dataframe with datetime index
    df = pd.DataFrame(index = [date.strftime("%Y-%m-%d")])
    df.index.name = "date"
    
    #Fetch details from the Database
    data = fetch_exchange_rates(date)
    
    #If not found hit the API and fetch from the servers
    if(len(data) == 0):
        logging.info("Getting Exchange rates via server for date = {}".format(date.strftime("%Y-%m-%d")))
        try:
            exchange_rate_url = "{base_url}/{date}?access_key={access_key}".format(base_url = url_exchange_rates, date = date.strftime("%Y-%m-%d"), access_key = access_key_exchange)
            res = requests.get(exchange_rate_url)
            resp = res.json()
            if(res.status_code == 200):
                df.insert(0, "exchange_rates", json.dumps(resp["rates"]))
                df.insert(1, "base_currency", base_currency_exchange)
                df.to_sql("exchange_rate", engine, if_exists = 'append')
                temp = resp["rates"][currency]
            return temp/resp["rates"][base_currency_market]
        except:
            logging.error("Server not reachable")
            return -1
    else:
        temp = json.loads(data[0][0])
        return temp[currency]/temp[base_currency_market]
    
def return_data(dataframe, currency):
    
    # If required currency is base currency then return the same dataframe
    if(currency == base_currency_market):
        logging.info("Results: \n {} \n".format(dataframe))
        return {"status":"success"}
    
    # get exchange rate and convert into required currency
    else:
        #iterate through dataframe
        for i, row in dataframe.iterrows():
            ex_rate = get_exchange_rate(i, currency)
            if(ex_rate > 0):
                # Change price to converted price
                converted_price =  round(float(row["price"])*float(ex_rate), 2)
                dataframe.at[i, 'price'] = converted_price
            else:
                logging.error("Internal service error")
                return None
            #sleep for 1 second due to API limitations
            sleep(1)
            
        logging.info("Results: \n {} \n".format(dataframe))
        return {"status":"success"}
        

def get_data(symbol, currency, start_date, end_date):
    logging.info("Getting details...")
    timeseries = pd.date_range(start_date, end_date)
    df = pd.DataFrame(index = timeseries)
    df.index.name = "date"
    df.insert(0, column = "price", value = None)
    df_database = df.copy()
    df_url = df.copy()
    
    data = fetch_market_data(symbol, start_date, end_date)
    for i in range(len(data)):
        df_database.loc[data[i][2]] = data[i][1]
    df_size = df_database.dropna().size
    df_database = df_database.dropna()
    df_database.insert(1, "symbol", symbol)
    if(df_size < (end_date-start_date).days+1):
        logging.info("Fetching details from the server...")
        try:
            url = "{base_url}/{feature}?access_key={access_key}&symbols={symbol}&date_from={start}&date_to={end}".format(base_url = url_market_price, feature = stock_price_feature, access_key = access_key_market, symbol = symbol, start = start_date, end = end_date)
            res = requests.get(url)
            resp = res.json()
            logging.info("Processing your request") 
            if(res.status_code == 200):
                count = resp["pagination"]["count"]
                for i in range(count):
                    temp = resp["data"][i]
                    temp["date"] = temp["date"].split("T")[0]
                    df_url.loc[str(datetime.strptime(temp["date"], "%Y-%m-%d").strftime("%Y-%m-%d"))] = temp["close"]
                df_url = df_url.replace([None], -1)
                df_url.insert(1, "symbol", symbol)
                df_url.insert(2, "currency", base_currency_market)
                df3 = pd.concat([df_database, df_url])
                df3 = df3[~df3.index.duplicated(keep = False)]
                # Store data into the database for market_price
                df3.to_sql("market_price", engine, if_exists = 'append')
                df_url = df_url.loc[df_url["price"] >= 0]
                return return_data(df_url, currency)
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
        df_database = df_database.loc[df_database["price"] >= 0]
        return return_data(df_database, currency)
   
            
def input_and_validate():
    logging.info("Verifying your inputs")
    symbol = "AAPL"
    currency = "INR"
    start = "2022-03-06"
    end = "2022-03-10"
    
    ############# commnd line argumanets ####################
    # parser = argparse.ArgumentParser(description = 'Market Data')
    # parser.add_argument(" -- symbol", type = str)
    # parser.add_argument(" -- currency", type = str)
    # parser.add_argument(" -- start", type = str)
    # parser.add_argument(" -- end", type = str)
    # args = parser.parse_args()
    
    # symbol = args.symbol
    # currency = args.currency
    # start = args.start
    # end = args.end
    
    #########################################################
    #Input
    # symbol = input("Enter Stock symbol: ")
    #Input validation
    if(symbol is None or len(symbol.strip()) == 0):
        logging.error("Error in symbol, Please enter correct symbol of stock")
        return {"status":"failed"}
    
    # currency = input("Enter Currency Symbol: ")
    #Input validation
    if(currency is None or len(currency.strip()) == 0):
        logging.error("Error in currency, Please enter correct symbol of currency")
        return {"status":"failed"}
    # start = input("Enter Start date: ")
    # end = input("Enter end date: ")
    try:
        start_date = datetime.strptime(start, "%Y-%m-%d")
    except:
        logging.error("Error in start date, Start date should be in format of YYYY-MM-DD")
        return {"status":"failed"}
    try: 
        end_date = datetime.strptime(end, "%Y-%m-%d")
    except:
        logging.error("End date should be in format of YYYY-MM-DD")
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
    
