import polars as pl

def main():
    compustat = pl.read_csv('E:/COMPUSTAT/ticker_details_csv/daily_tickers.csv', infer_schema_length=100000)
    compustat = compustat.drop(['gvkey', 'iid', 'datadate'])

    unique_compustat = compustat.unique()

    unique_compustat.write_csv('E:/COMPUSTAT/ticker_details_csv/unique_tickers.csv')

    pause = 1


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
"""