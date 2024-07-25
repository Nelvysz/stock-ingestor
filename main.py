import requests
import pandas as pd
import yfinance as yf
from datetime import timedelta
from datetime import date
from config import *
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine

conn = create_engine(
    url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
        DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
    )
)

def fetch_company_symbols() -> pd.DataFrame:
    response = requests.post(GLOBAL_SCAN_API_URL, json=GLOBAL_SCAN_API_BODY)
    if response.status_code == 200:
        data = response.json()['data']
        df = pd.DataFrame.from_records(data)
        df[['symbol', 'company', 'exchange']] = df['d'].to_list()
        return df[['company', 'symbol']]
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None


def extract_price_max_date() -> str:
    query = f"""
        select Symbol ,max(Date) as max_date
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
        select Name,Symbol ,max(Date) as Max_date
        from {DB_INDICATOR_TABLE}
        group by Name,Symbol
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
        price_df = ticker.history(start=start_date, period="max", interval='1m').reset_index(
        ).rename(columns={'Datetime': 'Dt'})
        if not price_df.empty:
            price_df['Date'] = price_df['Dt'].dt.date
            price_df['Time'] = price_df['Dt'].dt.time
            price_df['Symbol'] = symbol
            price_df['Exchange'] = ticker.get_info()['exchange']
            price_df = price_df[['Symbol', 'Exchange', 'Open',
                                'High', 'Low', 'Close', 'Volume', 'Date', 'Time']]
            price_df.to_sql(DB_GLOBAL_PRICE_TABLE, con=conn,
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
            symbol, cashflow_df, max_date_indicator_df, "Balance sheet")
        balancesheet_df = transform_indicator_df(
            symbol, balancesheet_df, max_date_indicator_df, "Cash flow")

        result_df = pd.concat(
            [income_df, cashflow_df, balancesheet_df], ignore_index=True)
        result_df = result_df[['Name', 'Symbol', 'Value', 'Date', 'Type']]
        result_df.to_sql(DB_INDICATOR_TABLE, con=conn,
                         if_exists='append', index=False)
    else:
        return symbol  # Return symbol to mark it as having no data


def extract_global_price_multithread(symbol_df: pd.DataFrame) -> pd.DataFrame:
    max_date_price_df = extract_price_max_date()
    max_date_indicator_df = extract_indicator_max_date()
    symbol_list = symbol_df['symbol'].to_list()

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {executor.submit(extract_symbol_price_indicator, symbol,
                                            max_date_price_df, max_date_indicator_df, conn): symbol for symbol in symbol_list}

        for future in as_completed(future_to_symbol):
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

if __name__ == "__main__":
    symbol_df = fetch_company_symbols()
    extract_global_price_multithread(symbol_df)
