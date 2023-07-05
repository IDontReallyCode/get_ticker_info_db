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
    In that case, I extract the original ticker and only store that one.
    
    The standard for index tickers is different also. When using the ticker "as is", tda-api returns the data and the "exchange" is "ind". 
    So, this is how I can identify indexes
    
    The list contains tickers that have since dissapeared because M&A or delistings.
    I have yet to find a way to get the data for those.
"""

import polars as pl
import psycopg2
import psycopg2.extensions as pgext
import pickle
from tda.auth import easy_client
from tda.client import Client
from config import CONSUMER_KEY, REDIRECT_URI, JSON_PATH
import time
from datetime import datetime

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


def SQL_INSERT_TICKER_DETAILS(cnxn:pgext.connection, tickdata:dict):
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
    
    # get CRSP data into a polars dataframe
    compustat = pl.read_csv('E:/COMPUSTAT/ticker_details_csv/unique_tickers.csv')
    
    cnxn = SQL_CONNECT()
        
    for ticker in tickerlist:
        
        foundit = compustat.filter(pl.col('tic')==ticker)
        if len(foundit)>0:
            if foundit['tpci'][0] in ['0', '1', 'Q', '4']:
                assettype = 'EQUITY'
            elif foundit['tpci'][0] in ['A', 'B']:
                assettype = 'INDEX'
            elif foundit['tpci'][0] in ['7', 'E', '%', '+']:
                assettype = 'ETF'
            elif foundit['tpci'][0] in ['R']:
                assettype = 'STRUCTURED'
            elif foundit['tpci'][0] in ['F', 'S']:
                assettype = 'DEPOSITORY RECEIPT'
            else:
                pauseerror=1

            exchange = 'DEAD'
            mylittledict = {'symbol':ticker, 'cusip':foundit['cusip'][0], 'description':foundit['conm'][0], 'assetType':assettype, 'exchange':exchange}
            # tickdata['symbol'], tickdata['cusip'], tickdata['description'], tickdata['assetType'], tickdata['exchange']
            # for ticker in tickerlist:
            SQL_INSERT_TICKER_DETAILS(cnxn, mylittledict)
        else:
            print(f"{ticker} was not found ???")
                
        pass
    
    pass


if __name__ == '__main__':
    main()

    
"""
    Code
	

Description

0 Common, ordinary

1 Preferred, preference, etc.

7 Mutual or investment trust fund

A Market index

B Equity or index option

E Unit investment trust (UIT)

Q Special Stock

% Exchange Traded Fund

+ Open End Fund

R Structured Product

F Depository Receipt

4 Unit - an issue that is comprised of a combination of common shares and warrants
"""
