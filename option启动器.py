import aiohttp
import asyncio
import numpy as np
import os
import pandas as pd
import pandas_datareader as pdr
import re
import sys
import time
from fake_useragent import UserAgent
import requests as req
import schedule

project_dir = '/Users/luyijie/Desktop/option监测器/'
sys.path.append(project_dir)
import option_reader

option_reader.whole_job()
'''
schedule.every().day.at('09:30').do(option_reader.whole_job()) #每天09:30顺次执行工作
schedule.every(15).minutes.do(option_reader.whole_job())

while True: #循环监测，一到时间自动开始工作
    schedule.run_pending()
    time.sleep(1)
'''