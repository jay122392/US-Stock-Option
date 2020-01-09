import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def draw_plot(symbol,price,expiry,type):
    org_file = pd.read_csv('{}_{}_{}_{}.csv'.format(symbol,price,expiry,type)) 

    option_price_set = org_file['lastPrice']
    stock_price_set = org_file['underlyingPrice']
    dt_set = org_file['time']

    ##Show visiable charts
    fig,ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.plot(dt_set,option_price_set,'g',marker = 'o')
    ax2.plot(dt_set,stock_price_set,'b',marker = 'o')

    ax1.set_xlabel('Date & Time')
    ax1.set_ylabel('Option Price',color = 'g')
    ax2.set_ylabel('Stock Price',color = 'b')
    ax1.set_title('{}_{}_{}_{}'.format(symbol,price,expiry,type))
    ax1.set_xticklabels(dt_set, rotation=90,fontsize = 5)
    plt.legend(loc='best')

    plt.show()

draw_plot('TSLA',330,2019-12-27,'Call')