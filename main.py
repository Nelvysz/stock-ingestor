import requests
import pandas as pd
import yfinance as yf
import contextlib
import logging
import time
import schedule
from datetime import timedelta
from datetime import date
from config import *
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert
from concurrent.futures import ThreadPoolExecutor, as_completed
from ta import add_all_ta_features
from ta.utils import dropna
from tqdm import tqdm
import contextlib
import io

logger = logging.getLogger('yfinance')
logger.disabled = True
logger.propagate = False

conn = create_engine(
    url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
        DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
    )
)

def fetch_company_symbols() -> pd.DataFrame:
    print('fetch data from tradingview ...')
    response = requests.post(GLOBAL_SCAN_API_URL, json=GLOBAL_SCAN_API_BODY)
    if response.status_code == 200:
        data = response.json()['data']
        df = pd.DataFrame.from_records(data)
        df[['symbol', 'company', 'exchange']] = df['d'].to_list()
        print(f'{df.shape[0]} symbols are fetching ...')
        return df[['company', 'symbol']]
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None


def extract_price_max_date() -> str:
    query = f"""
        select Symbol, max(Date) as max_date
        from {DB_GLOBAL_PRICE_TABLE}
        group by Symbol
    """
    try:
        result = pd.read_sql(query, conn)
        return result
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)


def extract_indicator_max_date() -> str:
    query = f"""
        select Name, Symbol, max(Date) as Max_date
        from {DB_INDICATOR_TABLE}
        group by Name, Symbol
    """
    try:
        result = pd.read_sql(query, conn)
        return result
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)


def transform_indicator_df(symbol: str, source_df: pd.DataFrame, max_date_df: pd.DataFrame, indicator_type: str) -> pd.DataFrame:
    df = pd.melt(source_df, id_vars=[
                 'Name'], var_name="Timestamp", value_name="Value").sort_values(by=['Name'])
    df['Symbol'] = symbol
    df['Type'] = indicator_type
    df['Date'] = pd.to_datetime(df['Timestamp'])
    if not max_date_df[max_date_df['Symbol'] == symbol].empty:
        df = df.merge(max_date_df, on=['Name', 'Symbol'])
        df = df[df['Date'] > df['Max_date']]
    return df


def extract_symbol_price_indicator(symbol: str, max_date_price_df: pd.DataFrame, max_date_indicator_df, conn):
    ticker = yf.Ticker(symbol)
    max_date = max_date_price_df.loc[max_date_price_df['Symbol']
                                    == symbol, "max_date"].to_list()
    start_date = max_date[0] if max_date else None
    today = date.today()
    if start_date:
        start_date = start_date + timedelta(days=1)
    if start_date is None or today > start_date:
        with contextlib.redirect_stdout(io.StringIO()):
            price_df = ticker.history(start=start_date, period="max", interval='1m').reset_index().rename(columns={'Datetime': 'Dt'})
            if not price_df.empty:
                price_df['Date'] = price_df['Dt'].dt.date
                price_df['Time'] = price_df['Dt'].dt.time
                price_df['Symbol'] = symbol
                price_df['Exchange'] = ticker.get_info()['exchange']
                price_df = price_df[['Symbol', 'Exchange', 'Open',
                                    'High', 'Low', 'Close', 'Volume', 'Date', 'Time']]
                
                price_df = dropna(price_df)
                price_indicator_df = add_all_ta_features(
                price_df, open="Open", high="High", low="Low", close="Close", volume="Volume")
                list_to_filter = price_indicator_df.columns
                elements_to_keep = ['Exchange','Open', 'High','Low','Close','Volume','Date','Time']
                indicator_list = [element for element in list_to_filter if element not in elements_to_keep]
                
                price_df = price_df[['Symbol', 'Exchange', 'Open',
                                    'High', 'Low', 'Close', 'Volume', 'Date', 'Time']]
                
                indicator_df = price_indicator_df[[*indicator_list,'Date','Time']]
                
                price_df.to_sql(DB_GLOBAL_PRICE_TABLE, con=conn,
                                if_exists='append', index=False)
                
                indicator_df.to_sql(DB_LOCAL_TECHNICAL_TABLE, con=conn,
                                if_exists='append', index=False)
                
    if not ticker.get_incomestmt().empty:
        income_df = ticker.get_incomestmt(
            freq='quarterly').reset_index().rename(columns={'index': 'Name'})
        cashflow_df = ticker.get_cashflow(
            freq='quarterly').reset_index().rename(columns={'index': 'Name'})
        balancesheet_df = ticker.get_balancesheet(
            freq='quarterly').reset_index().rename(columns={'index': 'Name'})

        income_df = transform_indicator_df(
            symbol, income_df, max_date_indicator_df, "Income Statement")
        cashflow_df = transform_indicator_df(
            symbol, cashflow_df, max_date_indicator_df, "Cash flow")
        balancesheet_df = transform_indicator_df(
            symbol, balancesheet_df, max_date_indicator_df, "Balance sheet")

        result_df = pd.concat(
            [income_df, cashflow_df, balancesheet_df], ignore_index=True)
        result_df = result_df[['Name', 'Symbol', 'Value', 'Date', 'Type']]
        result_df.to_sql(DB_INDICATOR_TABLE, con=conn,
                        if_exists='append', index=False)
    else:
        return symbol  # Return symbol to mark it as having no data


def extract_global_price_multithread(symbol_df: pd.DataFrame) -> pd.DataFrame:
    print('get lastest global price chart update ...  ')
    max_date_price_df = extract_price_max_date()
    print('get lastest indicator update ...  ')
    # max_date_indicator_df = extract_indicator_max_date()
    max_date_indicator_df = pd.DataFrame()
    print(f'{symbol_df.shape[0]} will be inserted in table')
    symbol_list = symbol_df['symbol'].to_list()
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {executor.submit(extract_symbol_price_indicator, symbol,
                                        max_date_price_df, max_date_indicator_df, conn): symbol for symbol in symbol_list}

        for future in tqdm(as_completed(future_to_symbol), total=len(future_to_symbol), desc="Processing symbols"):
            symbol = future_to_symbol[future]
            try:
                result = future.result()
                if result:
                    symbol_df.loc[symbol_df['symbol']
                                == symbol, 'symbol'] = None
            except Exception as exc:
                print(f'{symbol} generated an exception: {exc}')

    symbol_df.to_csv('company_symbol.csv')
    return symbol_df

def job():
    symbol_df = fetch_company_symbols()
    extract_global_price_multithread(symbol_df)

if __name__ == "__main__":
    schedule.every().day.at("00:00").do(job)  # Schedule the job every day at 1 AM
    while True:
        schedule.run_pending()
        time.sleep(1)