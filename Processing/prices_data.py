# First: pip install pip --upgrade
# Next: Install requirements: pip install -r Processing/requirements.txt
# Import necessary packages
from datetime import datetime
import yfinance as yf
from pandas_datareader import data as pdr
import pandas as pd
import os


ticker_names = ['Crude Oil', 'Gold', 'DOWIA', 'S&P500', 'VIX']
properties = ['Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']

headers = ['Date']
headers.extend([x + ' ' + y for y in properties for x in ticker_names])

# Use pandas_datareader to access Yahoo Finance data
yf.pdr_override()
# Declare start time as start of year 2000
start_datetime = datetime.strptime('2000-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
# Declare end time as current time when program is ran
end_datetime = datetime.now()


# Function to get specific price data from Yahoo Finance with default range of 2000-now and put in a specific csv file
def yf_data(ticks, csv, start=start_datetime, end=end_datetime, period='ytd', interval='1d'):
    pdr.get_data_yahoo(ticks, start=start, end=end, period=period, interval=interval).to_csv(csv)


# Function to get specific price data from Yahoo Finance with default range of the past year and append to a csv file
def yf_data_append(ticks, csv, period='1y', interval='1d'):
    pdr.get_data_yahoo(ticks, period=period, interval=interval).to_csv(csv, mode='a', header=False)


# Function to clean the uncleaned csv and store in a cleaned csv
def clean_uncleaned():
    # Read the uncleaned csv
    df = pd.read_csv('prices_2000_2021_uncleaned.csv', header=0, names=headers, index_col=0)
    # Remove all rows that don't have all necessary data and also rounds all values to 3 decimals places
    df = df.apply(pd.to_numeric, errors='coerce').round(3).dropna()
    # Put the processed data into prices_2000_2021_cleaned.csv
    df.to_csv('prices_2000_2021_cleaned.csv')


# Get all price data from Yahoo Finance (2000-now) in 1 day intervals in USD
def get_prices():
    # Get all price data from Yahoo Finance (2000-now) and put it in prices_2000_2021_uncleaned.csv
    yf_data('GC=F ^GSPC ^DJI CL=F ^VIX', 'prices_2000_2021_uncleaned.csv')

    # Obtain the cleaned csv from the uncleaned csv
    clean_uncleaned()

    # Makes path to the folder called "Individual Data"
    folder_path = os.path.join(os.getcwd(), 'Individual Data')
    # Try making the folder if it does not exist
    try:
        os.mkdir(folder_path)
    # Print out the error (Folder already exists means everything is fine)
    except OSError as Error:
        print(Error)
    # Goes to the directory of the folder
    os.chdir(folder_path)

    # Get Gold price data from Yahoo Finance (2000-now) and put it in prices-gold_2000_2021.csv
    yf_data('GC=F', 'prices-gold_2000_2021.csv')

    # Get S&P 500 price data from Yahoo Finance (2000-now) and put it in prices-S&P500_2000_2021.csv
    yf_data('^GSPC', 'prices-S&P500_2000_2021.csv')

    # Get Dow Jones Industrial Average price data from Yahoo Finance (2000-now) and put it in prices-DOWIA_2000_2021.csv
    yf_data('^DJI', 'prices-DOWIA_2000_2021.csv')

    # Get Crude Oil price data from Yahoo Finance (2000-now) and put it in prices-crude-oil_2000_2021.csv
    yf_data('CL=F', 'prices-crude-oil_2000_2021.csv')

    # Get VIX price data from Yahoo Finance (2000-now) and put it in prices-VIX_2000_2021.csv
    yf_data('^VIX', 'prices-VIX_2000_2021.csv')


# Update prices for one specific csv with specific tickers
def update_prices(tickers, csv, head=1, name_list=['Date', 'Open', 'High', 'Low',  'Close', 'Adj Close', 'Volume']):
    yf_data_append(tickers, csv)
    df = pd.read_csv(csv, header=head, names=name_list, index_col=0)

    # Remove all duplicates, rows that don't have all necessary data, and also rounds all values to 3 decimals places
    df = df[~df.index.duplicated(keep='last')].apply(pd.to_numeric, errors='coerce').round(3).dropna()
    # Put the processed data into csv
    df.to_csv(csv)


# Update all prices in a one year interval
def update_all_prices():
    # Updates all price data from Yahoo Finance for one year and put it in prices_2000_2021_uncleaned.csv
    update_prices('GC=F ^GSPC ^DJI CL=F ^VIX', 'prices_2000_2021_uncleaned.csv', head=0, name_list=headers)

    # Obtain the cleaned csv from the uncleaned csv
    clean_uncleaned()

    # Makes path to the folder called "Individual Data"
    folder_path = os.path.join(os.getcwd(), 'Individual Data')

    # Goes to the directory of the folder
    os.chdir(folder_path)

    # Update Gold price data from Yahoo Finance for one year and put it in prices-gold_2000_2021.csv
    update_prices('GC=F', 'prices-gold_2000_2021.csv')

    # Update S&P 500 price data from Yahoo Finance one year and put it in prices-S&P500_2000_2021.csv
    update_prices('^GSPC', 'prices-S&P500_2000_2021.csv')

    # Update Dow Jones Industrial Average price data from Yahoo Finance for one year and put it in
    # prices-DOWIA_2000_2021.csv
    update_prices('^DJI', 'prices-DOWIA_2000_2021.csv')

    # Update Crude Oil price data from Yahoo Finance for one year and put it in prices-crude-oil_2000_2021.csv
    update_prices('CL=F', 'prices-crude-oil_2000_2021.csv')

    # Update VIX price data from Yahoo Finance one year and put it in prices-VIX_2000_2021.csv
    update_prices('^VIX', 'prices-VIX_2000_2021.csv')


# Run method to get all data
# get_prices()

# Run method to update all data for one year
update_all_prices()
