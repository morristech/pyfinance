import bs4 as bs
import datetime as dt
import os
import pandas as pd
import pandas_datareader as web
import pickle
import requests
import time
from alpha_vantage.timeseries import TimeSeries


def save_sp500_tickers():
    resp = requests.get(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")

    soup = bs.BeautifulSoup(resp.text, "lxml")
    table = soup.find("table", {"class": "wikitable sortable"})
    tickers = []
    for row in table.findAll("tr")[1:]:  # skip header column
        ticker = row.findAll("td")[0].text
        tickers.append(ticker)

    with open("sp500.pickle", "wb") as f:
        pickle.dump(tickers, f)

    print(tickers)
    return tickers


def get_data_from_yahoo(reload_sp500=False):
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500.pickle", "rb") as f:
            tickers = pickle.load(f)

    if not os.path.exists("stock_dfs"):
        os.makedirs("stock_dfs")

    start = dt.datetime(2000, 1, 1)
    end = dt.datetime(2016, 12, 31)

    ts = TimeSeries(key=os.getenv("ALPHAVANTAGE_API_KEY"),
                    output_format="pandas")

    for ticker in tickers:
        print(ticker)
        if not os.path.exists("stock_dfs/{}.csv".format(ticker)):
            while True:
                try:
                    # df = web.DataReader(
                    #     ticker,
                    #     "av-daily",
                    #     start=start,
                    #     end=end,
                    #     access_key=os.getenv("ALPHAVANTAGE_API_KEY"),
                    # )

                    df, meta = ts.get_daily_adjusted(
                        symbol=ticker, outputsize="full")

                except Exception as e:
                    print(e)
                    time.sleep(1)
                else:
                    df.to_csv("stock_dfs/{}.csv".format(ticker))
                    break
        else:
            print("Already have {}".format(ticker))


def compile_data():
    with open("sp500.pickle", "rb") as f:
        tickers = pickle.load(f)

    main_df = pd.DataFrame()
    for count, ticker in enumerate(tickers):
        df = pd.read_csv("stock_dfs/{}.csv".format(ticker))
        df.set_index("date", inplace=True)
        df.rename(columns={"5. adjusted close": ticker}, inplace=True)
        df.drop(
            [
                "1. open",
                "2. high",
                "3. low",
                "4. close",
                "6. volume",
                "7. dividend amount",
                "8. split coefficient",
            ],
            1,
            inplace=True,
        )

        if main_df.empty:
            main_df = df
        else:
            main_df = main_df.join(df, how="outer")

        if count % 10 == 0:
            print(count)

    print(main_df.head())
    main_df.to_csv("sp500_joined_closes.csv")


# get_data_from_yahoo(reload_sp500=True)

compile_data()
