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
    
    # get CRSP data into a polars dataframe
    crsp = pl.read_csv('E:/CRSP/ticker_info.csv')
    
    cnxn = SQL_CONNECT()
    
    end_date_str = "2022-12-30"
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    beg_date_str = "2012-01-01"
    beg_date = datetime.strptime(beg_date_str, "%Y-%m-%d")
    
    for ticker in tickerlist:
        
        foundit = crsp.filter(pl.col('HTSYMBOL')==ticker)
        if len(foundit)>0:
            if datetime.strptime(foundit['ENDDAT'][0], "%Y-%m-%d") <= end_date:
                exchange = 'DEAD'
            else:
                why_did_I_end_up_here_I_should_already_have_data_on_this=1
                
            if datetime.strptime(foundit['ENDDAT'][0], "%Y-%m-%d") < beg_date:
                why_did_I_end_up_here_I_should_not_have_this_in_my_data=1
                exchange = 'DEAD'
                
            mylittledict = {'symbol':ticker, 'cusip':foundit['CUSIP'][0], 'description':foundit['HCOMNAM'][0], 'assetType':'EQUITY', 'exchange':exchange}
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
DLSTCD
Code	Category
100 	Active
200 	Mergers
300 	Exchanges
400 	Liquidations
500 	Dropped
600 	Expirations
900 	Domestics that became Foreign 
"""