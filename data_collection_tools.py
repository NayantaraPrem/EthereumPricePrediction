# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 23:46:38 2019

@author: Tara Prem

This module contains helper functions for collecting data for the project
""" 
import pytrends.dailydata as dd
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from getpass import getpass
import os
import re
import numpy as np

def download_daily_google_trends(keyword, start_year, start_month, end_year, end_month):
    """
    Query for and aggregate daily google search trends data for 'keyword' and
    download it as a CSV named 'google_trends_{keyword}_{timestamp}.csv'

    Args:
        keyword: (str) word to search for
        start)year: (int) returning trends starting from this year (and month)
        start_month: (int) returning trends starting from this (year and) month
        end_year: (int) returning trends ending at this year (and month)
        end_month: (int) returning trends ending at this (year and) month
    
    Returns:
        None
    
    Examples:
        download_daily_google_trends(keyword = 'ethereum', start_year=2015, start_month=7, end_year=2019, end_month=11)
    """
    #API doc  and math explained: https://github.com/GeneralMills/pytrends/blob/master/pytrends/dailydata.py
    df_daily = dd.get_daily_data(keyword, start_year, start_month, end_year, end_month)
    print(df_daily.tail(31))
    
    # plotting the data per month obtained from Google
    plt.plot(df_daily.index, df_daily[f"{keyword}_monthly"])
    plt.autoscale(enable=True, axis='x', tight=True)
    plt.title(f"Google trends (monthly data): {keyword}")
    plt.grid(True)
    plt.show()
    
    #plotting the daily data rescaled from the monthly data and the data in a month month 'APIs'
    plt.plot(df_daily.index, df_daily[f"{keyword}"])
    plt.autoscale(enable=True, axis='x', tight=True)
    plt.title(f"Google trends(rescaled to make the daily data comparable): {keyword}")
    plt.grid(True)
    plt.show()
    
    #download CSV of the dt
    timestamp = int(datetime.timestamp(datetime.now()))
    filename = f"google_trends_{keyword}_{timestamp}.csv"
    df_daily.to_csv(filename)
    return

def _parse_bitinfo_graph_record(record):
    date = record[11:21]
    value = record[24:-1]
    return np.array([date,value])

def download_bitinfo_graph_data(url, column_name):
    """
    Scrape and aggregate data from the graphs at bitinfocharts.com into 
    CSV named '{column_name}_{timestamp}.csv'

    Args:
        url: (str) URL to a graph at bitinfocharts.com
        column_name: (str) Name to assign the CSV file and column
    
    Returns:
        None
    
    Examples:
        download_bitinfo_graph_data(url='https://bitinfocharts.com/comparison/ethereum-tweets.html', column_name='ethereum_tweet_count')
    """
    response = requests.get(url)
    script_text = BeautifulSoup(response.text,'lxml').findAll('script')[5].text
    pattern = re.compile(r'\[new Date\("\d{4}/\d{2}/\d{2}"\),\d*\w*\]')
    records = pattern.findall(script_text)
    transactions = np.empty((0,2))
    for record in records:
        transactions = np.vstack((transactions, _parse_bitinfo_graph_record(record)))
    df_tweet = pd.DataFrame(transactions[:,1], index=transactions[:,0], columns=[f"{column_name}"])
    df_tweet.index = pd.to_datetime(df_tweet.index)
    print(df_tweet.tail(3))
    
    #plot the column_name count
    plt.plot(df_tweet.index, df_tweet[f"{column_name}"])
    plt.yscale('log')
    plt.title(f"{column_name}")
    plt.grid(True)
    
    #download CSV
    timestamp = int(datetime.timestamp(datetime.now()))
    filename = f"{column_name}_{timestamp}.csv"
    df_tweet.to_csv(filename)

def _format_exchange_data(rows):
    df = pd.DataFrame(rows[1:][:], columns = ["address", "name", "balance", "txn_count"]) # discard first empty row
    df['balance'] = df['balance'].apply(lambda str: float(str.strip(" Ether").replace(",", "")))
    df['txn_count'] = df['txn_count'].apply(lambda str: float(str.replace(",", "")))
    return df

def scrape_exchanges():
    """
    Scrapes etherscan for all the data on all the exchange addresses
    Return:
        (pd.DataFrame) DF with columns "address", "name", "balance", "txn count" 
    """
    page_number = 1
    page_limit=100
    exchanges = []
    while True:
        url = f"https://etherscan.io/accounts/label/exchange/{page_number}?ps={page_limit}"
        print(f"Requesting {url}")
        agent = {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0'}
        page = requests.get(url, headers=agent).text
        table = BeautifulSoup(page, 'html.parser').find('table')
        rows = [[item.text.strip() for item in row.find_all('td')] for row in table.find_all('tr')]
        if (len(rows) <= 2):
            # no more data
            break
        exchanges.append(_format_exchange_data(rows))
        page_number+=1
    exchanges = pd.concat(exchanges)
    print(exchanges.head())
    print(exchanges.count())
    print(exchanges.describe())
    return exchanges

def filter_top_exchange_addresses(exhanges_df, min_balance = 2000, min_txn_count = 400000):
    """
     Filter exchanges with balance > min_balance and transaction count > min_txn_count
    Args:
        exchanges_df: (pd.Dataframe) Exchanges with atleast columns 'balance'(float), 'txn_count'(float) and 'address' (str) 
        min_balance: (int) filter exchanges by balance > min_balance AND
        min_txn_count: (int) filter exchanges by txn count > min_txn_count
    Return: 
        (pd.Dataframe) of addresses
    Examples:
        df = scrape_exchanges()
        filter_top_exchange_addresses(df)
    """
    balance_condition = exhanges_df['balance'] > min_balance
    txn_condition = exhanges_df['txn_count'] > min_txn_count
    return exhanges_df[balance_condition & txn_condition]['address']

def get_txn_history(addresses, api_key = None, offset = 5000):
    """
    Returns all the historical transactions made to/from the input address.
    NOTE: there is a rate limit of 5 requests/sec for EtherScan.
    Args:
        addresses: (pd.DataFrame) addresses to return txns for
        api_key: (str) EtherScan.io API key. If None, will be prompted to enter one.
    Return:
        (dictionary of pd.DataFrame) DataFrames of transactions keyed by address of the transactions
    Examples:
        df = scrape_exchanges()
        df = filter_top_exchange_addresses(df)
        get_txn_history(df)
    """
    if (api_key is None):
        api_key  = getpass('Enter EtherScan.io API Key: ')

    txns_by_address = {}
    for address in addresses:
        page = 1
        txns = []
        while (page*offset <= 10000): # etherscan.io only maintains the last 10,000 txns
            api = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&page={page}&offset={offset}&sort=asc&apikey={api_key}"
            response = requests.get(api)
            if (response.json()['status'] != '1'):
                print(f"Failed API call: {response.json()['result']}; {response.json()['message']}")
                break
            print(f"page {page} @ {address}")
            txns.append(pd.DataFrame(response.json()['result']))
            page += 1
        txns_by_address[address] = pd.concat(txns)
    return txns_by_address
