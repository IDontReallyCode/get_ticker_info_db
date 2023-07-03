"""
    Open the file listing all tickers.
    Query the postgresql table containing the already loaded data
    difference the two series
    loop using the tda-api to get the info.

https://api.tdameritrade.com/v1/instruments

Example:
{
  "ABEV": {
    "cusip": "02319V103",
    "symbol": "ABEV",
    "description": "Ambev S.A. American Depositary Shares (Each representing 1 Common Share)",
    "exchange": "NYSE",
    "assetType": "EQUITY"
  }
}

    The list contains all tickers of any options traded on CBOE between 2012 and 2022.
    Because of corporate actions, such as stock splits, some option series use a variation of the ticker name.
    For example, ZZZ, and ZZZ1
    
    The standard for index tickers is different also. So I may need to "eff around to find out".
    
    The list contains tickers that have since dissapeared because M&A or delistings.
"""

import os
import polars as pl
import psycopg2
import pickle
import config
from tda.auth import easy_client
from tda.client import Client
from config import CONSUMER_KEY, REDIRECT_URI, JSON_PATH
import time

# Get some SQL connections ready
with open('sql.private', 'rb') as f:
    MYSQLPWD = pickle.load(f)
    
    
def SQL_CONNECT(dbname = 'optiondb', port='5432', password=MYSQLPWD):
    # Connect to Database
    ConnectionString = f"dbname= '{dbname}' user='QuantGuy' host='127.0.0.1' password='{MYSQLPWD}'  port={port}"

    try:
        cnxn = psycopg2.connect(ConnectionString)
    except psycopg2.Error as e:
        print(f"Can't connect to DATABASE: {e}")

    return cnxn


def SQL_SELECT_TICKERS_WITH_DETAILS()-> list:
    conn = SQL_CONNECT()
    
    # create a new cursor object
    cur = conn.cursor()
    
    # execute the SELECT statement
    cur.execute("SELECT ticker FROM ticker_detail")
    
    rows = cur.fetchall()
    
    listdone = [row[0] for row in rows]
    
    return listdone


def SQL_INSERT_TICKER_DETAILS(cnxn:psycopg2.extensions.connection, tickdata:dict):
    # Connect to Database
    # cnxn = SQL_CONNECT()
    cursor = cnxn.cursor()
   
    sql_InsertStatement = 'INSERT INTO public.ticker_detail (ticker, cusip, description, assettype, exchange) VALUES (%s, %s, %s, %s, %s)'

    if 'cusip' not in tickdata:
        tickdata['cusip'] = ''
        
    try:
        cursor.execute(sql_InsertStatement,
            (tickdata['symbol'], tickdata['cusip'], tickdata['description'], tickdata['assetType'], tickdata['exchange'])
            )
    except psycopg2.Error as e:
        print(f"Error inserting: {e}")

    cnxn.commit()  
    cursor.close()
      


def remove_extension(filename: str) -> str:
    ticker = '.'.join(filename.split('.')[:-1])
    
    return ''.join(char for char in ticker if not char.isdigit())


def main():
    # read the list of file names
    df = pl.read_ipc('list_all_ticker_files.ipc')

    # get list of tickers for which the work is done already
    listdone = SQL_SELECT_TICKERS_WITH_DETAILS()
    
    # extract ticker from filename
    df = df.with_columns([pl.col("filename").apply(remove_extension).alias("ticker")])
    
    # remove those that are done from the list
    df = df.filter(pl.col('ticker').is_in(listdone).is_not()) 
    
    tickerlist = df['ticker'].unique().to_list()
    
    # ticklist = tickerlist.iter_rows()   
    # tickers = df['ticker'].to_list()
    # tickers_string = ','.join(tickers)
    
    cnxn = SQL_CONNECT()
    
    for i in range(0, len(tickerlist), 10):
        # for ticker in tickerlist:
        start_time = time.time()        # this is used to throttle the process to stay within the limits of TDA
    
        # first, make sure I have the ticker, and not some notation of it
        letters_only = ','.join(tickerlist[i:i+10])
    
        c = easy_client(        api_key=CONSUMER_KEY,        redirect_uri=REDIRECT_URI,        token_path=JSON_PATH)
        r = c.search_instruments(symbols=letters_only, projection=Client.Instrument.Projection.SYMBOL_SEARCH)
        
        for key in r.json().keys():
            SQL_INSERT_TICKER_DETAILS(cnxn, r.json()[key])
        
        # if not bool(r.json()):
        #     # the search failed. Try for an index
        #     indexticker = '$'+ticker+'.X'
        #     r = c.search_instruments(symbols=indexticker, projection=Client.Instrument.Projection.SYMBOL_SEARCH)
        #     if not bool(r.json()):
        #         # not an index either
        #         # Leave it be, it will come out an "not done yet later"
        #         pass
        #     else:
        #         pausefordebug=1
        # else:
        #     SQL_INSERT_TICKER_DETAILS(cnxn, r.json()[ticker])
        
        delay = 0.6 - (time.time() - start_time)
        if delay>0:
            time.sleep(delay)

        pass
    
    pass


if __name__ == '__main__':
    main()

    