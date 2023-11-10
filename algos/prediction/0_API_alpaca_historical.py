import requests
import pandas as pd
import datetime as dt
from datetime import timedelta
from colorama import Back, Style, Fore
import time
import json
import _KEYS_DICT

# Alpaca endpoint URL  
API_KEY = "PKABCHC705K8SBM8MOR4"
API_SECRET = "gwXWcV6XhiVhgbqT2GKQIDRm0bWboh6tSgkUDni2"
APCA_API_BASE_URL = 'https://paper-api.alpaca.markets'
data_url = 'wss://data.alpaca.markets'
base_url = "https://data.alpaca.markets/v2/stocks/"

# Alpaca API keys
headers = {
    'APCA-API-KEY-ID': API_KEY,
    'APCA-API-SECRET-KEY': API_SECRET,
}

user_chosen_interval = input("Please enter your desired interval (1Min, 5Min, 30Min, 1Hour, etc.) : ")

def get_bars(symbol, start_date, end_date, timeframe=user_chosen_interval, limit=10000):
    df_list = []  # List to hold batches of dataframes
    page_token = None  # Page token for pagination

    while True:
        # Request parameters
        params = {
            'timeframe': timeframe,
            'start': start_date,
            'end': end_date,
            'limit': limit,
            'page_token': page_token,  # Add page_token to parameters
            # "type": "market",
        }

        # Make the GET request
        # print(f"{base_url}{symbol}/bars",params )
        response = requests.get(f"{base_url}{symbol}/bars", headers=headers, params=params)

        # Check the response for OK code
        if response.status_code != 200:
            print(f"{Fore.RED}\nFAILED RESPONSE {symbol}: {response.url}\n")
            print(f"{Fore.RED}Failed to fetch data for {symbol}: {response.content}  ErrorCode: : {response.status_code}\n")
            print(f"{Fore.YELLOW}DICT response headers {dict(response.headers)}\n")
            print(Fore.RESET)
            break

        data = response.json()

        if not data['bars']:
            print(f"{Fore.RED}No data found for {symbol} in the given date range.")
            break

        # Create a DataFrame only keeping 'Date', 'Open', 'High', 'Low', 'Close', 'Volume'
        df = pd.DataFrame(data['bars'])
        df = df[['t', 'o', 'h', 'l', 'c', 'v']]

        # Rename the columns as per your requirement
        df.rename(columns={'t':'Date', 'o':'Open', 'h':'High', 'l':'Low', 'c':'Close', 'v':'Volume'}, inplace=True)

        # Convert the 'Date' column to datetime format and set it as index
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)

        # Append the DataFrame to the list
        df_list.append(df)

        # If a next_page_token is present, continue fetching data, else break
        if 'next_page_token' in data and data['next_page_token'] is not None:
            page_token = data['next_page_token']
        else:
            print(Fore.YELLOW + "\nNo more pages to fetch.")
            break

    # Concatenate all the DataFrames in the list
    final_df = pd.concat(df_list)

    # Return the final DataFrame
    return final_df

# Set your parameters
# symbol = 'AAPL'  # Replace with the symbol you're interested in
START_DATE = '2017-01-01T00:00:00Z'
END_DATE = dt.datetime.utcnow().date().today() - dt.timedelta(days=1)

# END_DATE = '2023-11-01T23:59:59Z'
CSV_NAME = "@CHILL"
stocks_list = _KEYS_DICT.DICT_COMPANYS[CSV_NAME]

for symbol in stocks_list:
    # Fetch data
    print(Fore.GREEN + "\nStarting data fetching process... \nStock: ", symbol)
    df = get_bars(symbol, START_DATE, END_DATE)
    print(Fore.GREEN + "\nData fetching process completed... \ndf.shape: ", df.shape)
    
    # Save data as a CSV file
    if df is not None:
        TIME_ALPHA_OPEN = "13:00:00";TIME_ALPHA_CLOSE = "22:00:00";
        df = df.between_time(TIME_ALPHA_OPEN, TIME_ALPHA_CLOSE)
        df['Date'] = df.index
        max_recent_date = df.index.max().strftime("%Y%m%d")
        min_recent_date = df.index.min().strftime("%Y%m%d")
        print(Fore.CYAN + "\nd_price/alpaca/alpaca_" + symbol + '_' + user_chosen_interval + "_" + max_recent_date + "__" + min_recent_date + ".csv" + Fore.RESET)
        df.to_csv("d_price/alpaca/alpaca_" + symbol + '_' + user_chosen_interval + "_.csv",sep="\t", index=None)
        print(Fore.MAGENTA + "START: ", str(df.index.min()),  "\nEND: ", str(df.index.max()) , "\nSHAPE: ", df.shape, "\n")
    else:
        print(Fore.RED + "ERROR! None in Stock: ", symbol)