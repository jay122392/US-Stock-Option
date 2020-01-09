import pandas as pd
import datetime as dt

def create_original_file():
    data_file = pd.read_csv('option_price_latest.csv')
    symbol_file = pd.read_csv('M.csv')
    symbol_list = symbol_file['Symbol']
    price_list = symbol_file['PriceToFollow']
    expdates = set(data_file[data_file['daysToExpiration']<=30]['expirationDate'])
    for symbol,price in zip(symbol_list,price_list):
        for exp in expdates:
            dataframe = pd.read_csv('sample_org_file.csv')
            dataframe.to_csv('{}_{}_{}_Call.csv'.format(symbol,price,exp))
            dataframe.to_csv('{}_{}_{}_Put.csv'.format(symbol,price,exp))


def data_process():
    timeseries = []
    current_time = dt.datetime.now().strftime('%y-%m-%d %H:%M')
    original_file = pd.read_csv('option_price_latest.csv')
    for i in range(len(original_file)):
        timeseries.append(current_time)
    original_file['time'] = timeseries
    symbol_file = pd.read_csv('M.csv')
    symbol_list = symbol_file['Symbol']
    price_list = symbol_file['PriceToFollow']
    column_change = original_file.drop(['bidPrice','midpoint','askPrice','priceChange','percentChange','volatility','volume','openInterest'],axis = 1)
    one_month_filter = column_change[column_change['daysToExpiration']<=30]

    for symbol,price in zip(symbol_list,price_list):
        symbol_filter = one_month_filter[one_month_filter['symbol'] == symbol]
        exp_price_filter = symbol_filter[symbol_filter['strikePrice'] == price]
        call_df = exp_price_filter[exp_price_filter['optionType'] == 'Call']
        put_df = exp_price_filter[exp_price_filter['optionType'] == 'Put']
        # print(call_df)

        
        for expdate in list(call_df['expirationDate']):
            callfile = pd.read_csv('{}_{}_{}_Call.csv'.format(symbol,price,expdate))
            call_col = call_df[call_df['expirationDate'] == expdate]
            callfile = pd.concat([callfile,call_col])
            callfile.to_csv('{}_{}_{}_Call.csv'.format(symbol,price,expdate))

            putfile = pd.read_csv('{}_{}_{}_Put.csv'.format(symbol,price,expdate))
            put_col = put_df[put_df['expirationDate'] == expdate] 
            putfile = pd.concat([putfile,put_col])
            putfile.to_csv('{}_{}_{}_Put.csv'.format(symbol,price,expdate))
            
        

#create_original_file()
data_process()

