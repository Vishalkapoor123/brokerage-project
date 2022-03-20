from asyncio.log import logger
from datetime import datetime
import logging
from model import MarketPrice, ExchangeRate
from extensions import session, engine
import requests
import pandas as pd
import json
import argparse
from time import sleep


logging.getLogger().setLevel(logging.INFO)

access_key_market = "db0c82d397996b303c0cca45c603fded"
access_key_exchange = "353c659b37318718363726fa29e05532"
base_currency_exchange = "EUR"
base_currency_market = "USD"

#Get exchange rate by dates
def get_exchange_rate(date, currency):
    #Create a placeholder dataframe with datetime index
    df = pd.DataFrame(index=[date.strftime("%Y-%m-%d")])
    df.index.name = "date"
    
    #Fetch details from the Database
    data = session.query(ExchangeRate.rates, ExchangeRate.date).filter(ExchangeRate.date == date.strftime("%Y-%m-%d")).all()
    print(data)
    #If not found hit the API
    if(len(data)==0):
        logger.info("Getting Exchange rates via server for date={}".format(date.strftime("%Y-%m-%d")))
        try:
            url = "http://api.exchangeratesapi.io/v1/{}?access_key={}".format(date.strftime("%Y-%m-%d"),access_key_exchange)
            res = requests.get(url)
            resp = res.json()
            if(res.status_code ==200):
                df.insert(0, "rates", json.dumps(resp["rates"]))
                print(df)
                df.to_sql("exchange_rate", engine,if_exists='append')
                temp = resp["rates"][currency]
            return temp/resp["rates"][base_currency_market]
        except:
            logging.error("Server not reachable")
            return -1
    else:
        logger.info("Getting Exchange rates via database for date={}".format(date))
        temp = json.loads(data[0][0])
        return temp[currency]/temp[base_currency_market]
    
def return_data(dataframe, currency):
    if(currency == base_currency_market):
        dataframe.insert(2, "Currency", currency)
        logging.info(dataframe)
        return dataframe
    else:
        for i, row in dataframe.iterrows():
            ifor_val = get_exchange_rate(i, currency)
            if(ifor_val>0):
                dataframe.at[i,'rate'] = round(float(row["rate"])*float(ifor_val),2)
            sleep(1)
        dataframe.insert(2, "Currency", currency)
        logging.info("Results: \n {} \n".format(dataframe))
        return dataframe
        

def get_data(symbol, currency, start_date, end_date):
    logging.info("Getting details...")
    timeseries = pd.date_range(start_date, end_date)
    df = pd.DataFrame(index=timeseries)
    df.index.name = "date"
    df.insert(0,column="rate", value=None)
    df_database = df.copy()
    df_url = df.copy()
    
    data = session.query(MarketPrice.name, MarketPrice.rate, MarketPrice.date).filter(MarketPrice.name == symbol, MarketPrice.date >=start_date, MarketPrice.date <= end_date).all()
    for i in range(len(data)):
        df_database.loc[data[i][2]] = data[i][1]
    df_size = df_database.dropna().size
    df_database = df_database.dropna()
    df_database.insert(1, "name", symbol)
    if(df_size<(end_date-start_date).days+1):
        logging.info("Fetching details from the server...")
        try:
            url = "http://api.marketstack.com/v1/eod?access_key={}&symbols={}&date_from={}&date_to={}".format(access_key_market, symbol, start_date, end_date)
            res = requests.get(url)
            resp = res.json()
            if(res.status_code==200):
                count = resp["pagination"]["count"]
                for i in range(count):
                    temp = resp["data"][i]
                    temp["date"] = temp["date"].split("T")[0]
                    df_url.loc[str(datetime.strptime(temp["date"], "%Y-%m-%d").strftime("%Y-%m-%d"))] = temp["close"]
                df_url = df_url.replace([None], -1)
                df_url.insert(1, "name", symbol)
                df3 = pd.concat([df_database, df_url])
                df3 = df3[~df3.index.duplicated(keep=False)]
                # Store data into the database for market_price
                df3.to_sql("market_price", engine,if_exists='append')
                df_url = df_url.loc[df_url["rate"]>=0]
                return_data(df_url, currency)
        except:
            logging.error("Server not reachable")
            return {"status":"fail"}
        else:
            if(res.status_code==500):
                logging.error("Server is not reachable, sorry for the inconvinience")
                # return {"status":500, "info":"server error"}
                return {"status":"fail"}
            elif(res.status_code==422):
                logging.error("Invalid symbol, {} is not a valid stock symbol. Please check again".format(symbol))
                # return {"status":404, "info":"invalid symbol code"}
                return {"status":"fail"}
            else:
                return {"status":"fail"}
    else:
        df_database = df_database.loc[df_database["rate"]>=0]
        return_data(df_database, currency)
        return {"status":"success"}
   
            
def input_and_validate():
    logging.info("Verifying your inputs")
    symbol = "AAPL"
    currency = "INR"
    start = "2022-03-10"
    end = "2022-03-20"
    
    ############# commnd line argumanets ####################
    # parser = argparse.ArgumentParser(description='Market Data')
    # parser.add_argument("--symbol", type=str)
    # parser.add_argument("--currency", type=str)
    # parser.add_argument("--start", type=str)
    # parser.add_argument("--end", type=str)
    # args = parser.parse_args()
    
    # symbol = args.symbol
    # currency = args.currency
    # start = args.start
    # end = args.end
    
    #########################################################
    #Input
    # symbol = input("Enter Stock symbol: ")
    #Input validation
    if(symbol is None or len(symbol.strip())==0):
        logging.error("Error in symbol, Please enter correct symbol of stock")
        return {"status":"failed"}
    
    # currency = input("Enter Currency Symbol: ")
    #Input validation
    if(currency is None or len(currency.strip())==0):
        logging.error("Error in currency, Please enter correct symbol of currency")
        return {"status":"failed"}
    # start = input("Enter Start date: ")
    # end = input("Enter end date: ")
    try:
        start_date= datetime.strptime(start, "%Y-%m-%d")
    except:
        logging.error("Error in start date, Start date should be in format of YYYY-MM-DD")
        return {"status":"failed"}
    try: 
        end_date=datetime.strptime(end, "%Y-%m-%d")
    except:
        logging.error("End date should be in format of YYYY-MM-DD")
        return {"status":"failed"}
    
    #Date validation
    if(start_date>end_date):
        logging.error("Invalid dates, start date cannot be greater than end date")
        return {"status":"failed"}
    else:
        #Get data from databases/APIs
        get_data(symbol, currency, start_date, end_date)
        return {"status":"success"}      
            

# Main method
if __name__ == '__main__':
    input_and_validate()
    
