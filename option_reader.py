import aiohttp
import asyncio
import numpy as np
import os
import pandas as pd
import pandas_datareader as pdr
import re
import ssl
import sys
import time
from fake_useragent import UserAgent
import requests as req

'''set path variables'''
project_dir = '/Users/luyijie/Desktop/option监测器/'
sys.path.append(project_dir)
import async_option_scraper
import option_parser

def whole_job():
    #执行部分语句
    #=====================================
    today = pd.datetime.today().date()
    #=====================================
    file_start = time.time()
    print('\nAsync Barchart Scraper starting...')
    # ------------------- \\\
    # import symbols
    FILE = project_dir + '/M.csv' #自选股列表文件名
    ALL_ETFS = pd.read_csv(FILE)['Symbol']
    drop_symbols = ['AAPL'] #本次不想查看的option
    ETFS = [x for x in ALL_ETFS if x not in set(drop_symbols)]
    print(ETFS)

    # ===============================================
    # LOG IN TO BARCHART
    # ===============================================
    url = 'https://www.barchart.com/login'
    post_data = {'email':'luyijie1223@gmail.com','password':'ws02781358'}
    session = req.session()
    session.post(url,data= post_data)

    # ===============================================
    # GET HTML SOURCE FOR LAST SYMBOL EQUITY PRICE
    # ===============================================
    t0_price = time.time()
    # ----------------- \\\
    loop = asyncio.get_event_loop()

    px_scraper = async_option_scraper.Last_price_scraper() 
    px_run_future = asyncio.ensure_future(px_scraper.run(ETFS))

    loop.run_until_complete(px_run_future)
    px_run = px_run_future.result()

    # ----------------- ///
    duration_price = time.time() - t0_price
    print('\nprice scraper script run time:',
    pd.to_timedelta(duration_price,unit='s'))
    # ----------------- ///
    # create price dictionary
    px_dict = {}
    for k,v in zip(ETFS,px_run):
        px_dict[k] = v

    # =========================
    # RUN FIRST ASYNC SPARKER
    # =========================
    t0_first = time.time()
    # --------------  \\\
    ua = UserAgent()
    loop = asyncio.get_event_loop()

    first_scraper = async_option_scraper.First_async_scraper() 
    first_run_future = asyncio.ensure_future(first_scraper.run(ETFS,ua.random))

    loop.run_until_complete(first_run_future)
    first_run = first_run_future.result()
    # --------------  ///
    first_duration = time.time() - t0_first
    print('\nfirst async scraper script run time:',
    pd.to_timedelta(first_duration,unit = 's'))

    # ==========================================
    # EXTRACT EXPIRYS FROM FIRST RUN SCRAPER
    # ==========================================
    xp = async_option_scraper.Expirys(ETFS,first_run)
    expirys = xp.get_expirys()

    # ==========================================
    # SCRAPE AND AGGREGATE ALL SYMBOLS BY EXPIRY
    # ==========================================
    t0_xp = time.time()
    # ------------------- \\\
    # dict key = sym,values = list of json data by expiry
    # create helper logic to test if expirys is None before passing
    sym_xp_dict = {}
    ua = UserAgent()
    xp_scraper = async_option_scraper.Xp_async_scraper()
    for symbol in ETFS:
        print()
        print('-'*50)
        print('scraping: ',symbol)
        if not expirys[symbol]:
            print('symbol ' + symbol + ' missing expirys')
            continue
        try:
            xp_loop = asyncio.get_event_loop()
            xp_future = asyncio.ensure_future(
                xp_scraper.xp_run(symbol,expirys[symbol], ua.random)
                )
            xp_loop.run_until_complete(xp_future)
            sym_xp_dict[symbol] = xp_future.result()
        except Exception as e:
            print(symbol + ' error: ' + str(e))
    # ------------  ///
    duration_xp = time.time() - t0_xp
    print('\nall async scraper script run time: ',
        pd.to_timedelta(duration_xp, unit='s'))

    # ================================================
    # PARSE ALL COLLECTED DATA
    # ================================================
    t0_agg = time.time()
    # -------------- \\\
    all_etfs_data = []
    for symbol, xp_list in sym_xp_dict.items():
        print()
        print('-' *50)
        print('parsing: ', symbol)
        list_dfs_by_expiry = []
        
        try:
            for i in range(len(xp_list)):
                try:
                    parser = option_parser.option_parser(
                    symbol, xp_list[i])
                    call_df = parser.create_call_df()
                    put_df = parser.create_put_df()
                    concat = pd.concat([call_df,put_df],axis = 0)
                    concat['underlyingPrice'] = np.repeat(
                        parser.extract_last_price(px_dict[symbol]),
                        len(concat.index))
                    list_dfs_by_expiry.append(concat)
                except: continue
        except Exception as e:
            print(f'symbol: {symbol}\n error: {e}')
            print()
            continue
        all_etfs_data.append(pd.concat(list_dfs_by_expiry,axis = 0))
        print('number of dfs: ' + str(len(all_etfs_data)))
    # ----------  ///
    duration_agg = time.time() - t0_agg
    print('\nagg parse data script run time: ',
        pd.to_timedelta(duration_agg, unit = 's'))
    # ---------- \\\

    dfx = pd.concat(all_etfs_data , axis = 0).reset_index(drop = True)
    print(dfx.info())
    # ---------- ///
    '''
    # ================================================
    # GET ANY MISSING UNDERLYING PRICE
    # ================================================
    print('\nCollecting missing prices...')
    grp = dfx.groupby(['symbol'])['underlyingPrice'].count()
    missing_symbol_prices = grp[grp == 0].index

    get_price = lambda symbol: pdr.DataReader(symbol,'yahoo',today)['Adj Close']
    prices = []
    for symbol in missing_symbol_prices:
        px = get_price(symbol).iloc[0]
        prices.append((symbol,px))

    df_prices = pd.DataFrame(prices).set_index()
    for symbol in df_prices.index:
        (dfx.loc[dfx['symbol'] == symbol,
        ['underlyingPrice']]) = df_prices.loc[symbol].iloc[0]

    dfx['underlyingPrice'] = dfx.underlyingPrice.astype(float)
    print('\nmissing prices added')
    '''
    # ================================================
    # store dataframe as hdf
    # ================================================
    print(dfx.tail(20))
    print(dfx.info())

    file_duration =  time.time() - file_start
    print('\nfile script run time: ', pd.to_timedelta(file_duration, unit='s'))

    dfx.to_csv('option_price_latest.csv')