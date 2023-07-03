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
"""

import os
import polars as pl
import psycopg2
import pickle
import config

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


def SQL_GET_TICKERS_WITH_DETAILS()-> list:
    conn = SQL_CONNECT()
    
    # create a new cursor object
    cur = conn.cursor()
    
    # execute the SELECT statement
    cur.execute("SELECT ticker FROM ticker_detail")
    
    rows = cur.fetchall()
    
    listdone = [row[0] for row in rows]
    
    return listdone


def remove_extension(filename: str) -> str:
    return '.'.join(filename.split('.')[:-1])


def main():
    # read the list of file names
    tickerlist = pl.read_ipc('list_all_ticker_files.ipc')

    # get list of tickers for which the work is done already
    listdone = SQL_GET_TICKERS_WITH_DETAILS()
    
    # extract ticker from filename
    tickerlist = tickerlist.with_columns([pl.col("filename").apply(remove_extension).alias("ticker")])
    
    # remove those that are done from the list
    tickerlist = tickerlist.filter(pl.col('ticker').is_in(listdone).is_not())    
    
    
    pass


if __name__ == '__main__':
    main()

    