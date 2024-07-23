import requests
import pandas as pd
import schedule
import time
import yfinance as yf
import mysql.connector
import datetime
from config import *
from sqlalchemy import create_engine

conn =  create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
        )
    )

def fetch_company_symbols() -> pd.DataFrame:
    response = requests.post(GLOBAL_SCAN_API_URL, json=GLOBAL_SCAN_API_BODY)
    if response.status_code == 200:
        data = response.json()['data']
        df = pd.DataFrame.from_records(data)
        df[['symbol','company','exchange']] = df['d'].to_list()
        return df[['company','symbol']]
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

def extract_max_date() -> str:
    query = """
        select Symbol ,max(Date) as max_date
        from global_chart gc 
        group by gc.Symbol 
    """
    try :
        result = pd.read_sql(query, conn) 
        return result
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)
        
def extract_global_price():
    TABLE_NAME = 'global_chart'
    symbol_df = fetch_company_symbols()
    symbol_list = symbol_df['symbol'].to_list()
    max_date_df = extract_max_date()
    start_date = None
    for symbol in symbol_list:
        print(symbol)
        ticker = yf.Ticker(symbol)
        print(ticker)
        max_date = max_date_df.loc[max_date_df['Symbol'] == symbol,"max_date"].to_list()
        print(f'max_date : {max_date}')
        if max_date:
            start_date = max_date[0]
        df = ticker.history(start=start_date,period="max").reset_index().rename(columns={'Date' : 'Dt'})
        if not df.empty :
            df['Date'] = df['Dt'].dt.date
            df['Time'] = df['Dt'].dt.time
            df['Symbol'] = symbol
            df = df[['Symbol','Open','High','Low','Close','Volume','Date','Time']]
            df.to_sql(TABLE_NAME,con=conn,if_exists='append',index=False)
        else :
            symbol_df.loc[symbol_df['symbol'] == symbol,'symbol'] = None
    symbol_df.to_csv('company_symbol.csv')

# Schedule the job to run every hour
# schedule.every().hour.do(job)

if __name__ == "__main__":
    extract_global_price()
