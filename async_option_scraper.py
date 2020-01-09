import aiohttp
import asyncio
import ssl

class First_async_scraper:
    def __init__(self):
        pass

    async def _fetch(self,symbol,url,session,headers):
        ''' to retrieve option quotes as JSON
        Params:
            symbol : str(), ETF
            url : str(), request url
            session : aiohttp.ClientSession() object
            headers: dict() containning header info
        Returns:
            response : JSON/ Py Dict
        '''
        async with session.post(url.format(symbol), headers = headers) as response:
            return await response.json(content_type = None)

    async def run(self, symbols,user_agent):
        '''fn: to aggregate response option quotes
        Params:
            symbols : list of str(), ETF symbols
            user_agent : str()
        Returns:
            responses : list of JSON
        '''
        url = 'https://www.barchart.com/proxies/core-api/v1/options/chain?symbol={}&fields=strikePrice%2ClastPrice%2CpercentFromLast%2CbidPrice%2Cmidpoint%2CaskPrice%2CpriceChange%2CpercentChange%2Cvolatility%2Cvolume%2CopenInterest%2CoptionType%2CdaysToExpiration%2CexpirationDate%2CsymbolCode%2CsymbolType&groupBy=optionType&raw=1&meta=field.shortName%2Cfield.type%2Cfield.description'

        headers = {
            ':authority':'https://www.barchart.com',
            ':method':'GET',
            ':path': '/proxies/core-api/v1/options/chain?symbol={}&fields=strikePrice%2ClastPrice%2CpercentFromLast%2CbidPrice%2Cmidpoint%2CaskPrice%2CpriceChange%2CpercentChange%2Cvolatility%2Cvolume%2CopenInterest%2CoptionType%2CdaysToExpiration%2CexpirationDate%2CtradeTime%2CsymbolCode%2CsymbolType&groupBy=optionType&raw=1&meta=field.shortName%2Cfield.type%2Cfield.description',
            ':scheme':'https',
            'accept':'application/json',
            'accept-encoding':'gzip, deflate, br',
            'accept-language':'en-US,en;q = 0.9',
            'cookie': 'd7s_uid=k3gzmcy5xxlfmr; _gcl_au=1.1.1278243699.1574840919; _ga=GA1.2.1953878664.1574840919; __gads=ID=d3131d3af1c83e33:T=1574840920:S=ALNI_MYhU9WuojXXuBN04puWVy1yfwWvLg; kppid_managed=NEeY44ie; __qca=P0-1106577160-1574840927394; market=eyJpdiI6InNRN3lGUzB4U0ZDaHNRU3BsRHFYMnc9PSIsInZhbHVlIjoiRDZ5VGZMa0IwYUxhQ2VyMDdDYnBIQT09IiwibWFjIjoiNWJmMGY3NjA5NzFjNzMxMmQ0NjRjMGYxZTU5OTYyYTRhZDg3YmYxYTI3ZTZlNTNlODdjN2I0OGVkYTczYmVmOSJ9; _gid=GA1.2.1791915242.1578567999; __rtgt_sid=k56mo37fac8qav; d7s_spc=1; _awl=2.1578568073.0.4-2292a19e-520ac2c588eca8bc42c8f1549c858300-6763652d617369612d6561737431-5e170989-0; fitracking_2=yes; fiTrackingDomainParams=%7B%22host%22%3A%22tracking1.firstimpression.io%22%2C%22type%22%3A%22full%22%7D; fi_utm=direct%7Cdirect%7C%7C%7C%7C; _pk_ses.6495.73a4=*; XSRF-TOKEN=eyJpdiI6ImFoSnkrZXhibGZFRlFrRGkrRU55dEE9PSIsInZhbHVlIjoiakgrWkt5ZWs5cGYxOXMwK21Sdk4wbEhEZUVhYmFpQjBLRTZLOUREMTBuYklmcDRwZ1JqSzRtZDFTRHJVTGFwSCIsIm1hYyI6ImY4NTc5NjI2ZTU4NDBiMWE3MTE5YmViZWE0YTg1NjI4MGNiYTNiNWFlN2NhYTVkNDg0NTEzMGE2Njg2MTMxZWMifQ%3D%3D; laravel_session=eyJpdiI6IjNvOWE1V08yK2ZtQlNNRzlSdDlwaEE9PSIsInZhbHVlIjoicHRFNGhXMzhYUDBaV1NiY3dMVFwvR1wvU1NURlpRWE1qb3NpUzdEbEdrb0VJckZ4eDRvN0hjck43RlF1aHJoTm44IiwibWFjIjoiODMxZjlkOTJkZDJhMmMyNThiMWZlYjEzOGVkMjc1MjliYzQ1YmYyYmRkZTJkY2MxNThkNjY2ODJmMWM3ZGY1NyJ9; _pk_id.6495.73a4=468ed0dbee279aee.1578568080.1.1578568134.1578568080.; IC_ViewCounter_www.barchart.com=3; bcPromoPremierShowed=true; _gat_UA-2009749-51=1',
            'referer':'https://www.barchart.com/stocks/quotes/{}/options',
            'sec-fetch-mode':'cors',
            'sec-fetch-site':'same-origin',
            'x-xsrf-token': 'eyJpdiI6ImFoSnkrZXhibGZFRlFrRGkrRU55dEE9PSIsInZhbHVlIjoiakgrWkt5ZWs5cGYxOXMwK21Sdk4wbEhEZUVhYmFpQjBLRTZLOUREMTBuYklmcDRwZ1JqSzRtZDFTRHJVTGFwSCIsIm1hYyI6ImY4NTc5NjI2ZTU4NDBiMWE3MTE5YmViZWE0YTg1NjI4MGNiYTNiNWFlN2NhYTVkNDg0NTEzMGE2Njg2MTMxZWMifQ==',
            'User-Agent':user_agent
        }

        tasks = []
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                headers['referer'] = headers['referer'].format(symbol) #把symbol填充入Referer URL
                headers[':path'] = headers[':path'].format(symbol)
                task = asyncio.ensure_future(self._fetch(symbol,url,session,headers))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
            return responses
        #https://docs.python.org/3/library/asyncio-task.html#task-functions 有详解

# =========================获取expire date序列============================
class Expirys:
    def __init__(self,ETFS,first_future_result):
        '''Class to extract expiration data from Dict
        Params:
            ETFS : list of ETF symbol str()
            first_future_result : list of response objects (dict/JSON) from the firstscraper
        '''
        self.ETFS = ETFS
        self.first_future_result = first_future_result

    def _get_dict_expiry(self,response):
        '''fn: to get expirations from response dict
        Params:
            response : dict/JSON object
        Returns:
            list of date str(), YYYY-MM-DD
        '''
        if response['count'] == 0:
            return None
        else:
            return response['meta']['expirations']
        
        
        

    def get_expirys(self):
        '''fn: to create dict with k, v = symbol, list of expirys
            we have to do this because JSON/dict response data doesn't
            contain symbol identifier
        Returns:
            dict(symbol = list of expiry dates)
        '''
        from itertools import zip_longest
        expirys = {}
        for symbol,resp in zip_longest(self.ETFS, self.first_future_result):
            # We can do this because results are in order of submission instead of arrival
            # Gather returns responses in original order instead of arrival.
            # 参考官方说明：  https://docs.python.org/3/library/asyncio-task.html#task-functions
            expirys[symbol] = self._get_dict_expiry(resp)
        return expirys

class Xp_async_scraper:
    def __init__(self):
        pass

    async def _xp_fetch(self,symbol,expiry,url,session,headers):
        '''fn: to retrieve option quotes as JSON
        Params:
            symbol : str(), ETF
            expiry : str(), 'YYYY-MM-DD'
            url : str(), request url
            session : aiohttp.ClientSession() object
            headers : dict() containing header info
        Returns:
            response : JSON/Py Dict
        '''
        async with session.post(url.format(symbol,expiry), headers = headers) as response:
            return await response.json(content_type = None)

    async def xp_run(self,symbol,expirys,user_agent):
        '''fn : to aggregate response option quotes
        Params:
            symbol : str(), ETF
            expirys : list of date str() 'YYYY-MM-DD'
            user_agent : str()
        Returns:
            responsees : list of JSON
        '''
        url = 'https://www.barchart.com/proxies/core-api/v1/options/chain?symbol={}&fields=strikePrice%2ClastPrice%2CpercentFromLast%2CbidPrice%2Cmidpoint%2CaskPrice%2CpriceChange%2CpercentChange%2Cvolatility%2Cvolume%2CopenInterest%2CoptionType%2CdaysToExpiration%2CexpirationDate%2CsymbolCode%2CsymbolType&groupBy=optionType&expirationDate={}&raw=1&meta=field.shortName%2Cfield.type%2Cfield.description'

        headers = {
            ':authority':'https://www.barchart.com',
            ':method':'GET',
            ':path': '/proxies/core-api/v1/options/chain?symbol={}&fields=strikePrice%2ClastPrice%2CpercentFromLast%2CbidPrice%2Cmidpoint%2CaskPrice%2CpriceChange%2CpercentChange%2Cvolatility%2Cvolume%2CopenInterest%2CoptionType%2CdaysToExpiration%2CexpirationDate%2CtradeTime%2CsymbolCode%2CsymbolType&groupBy=optionType&expirationDate={}&raw=1&meta=field.shortName%2Cfield.type%2Cfield.description',
            ':scheme':'https',
            'accept':'application/json',
            'accept-encoding':'gzip, deflate, br',
            'accept-language':'en-US,en;q = 0.9',
            'cookie': 'd7s_uid=k3gzmcy5xxlfmr; _gcl_au=1.1.1278243699.1574840919; _ga=GA1.2.1953878664.1574840919; __gads=ID=d3131d3af1c83e33:T=1574840920:S=ALNI_MYhU9WuojXXuBN04puWVy1yfwWvLg; kppid_managed=NEeY44ie; __qca=P0-1106577160-1574840927394; market=eyJpdiI6InNRN3lGUzB4U0ZDaHNRU3BsRHFYMnc9PSIsInZhbHVlIjoiRDZ5VGZMa0IwYUxhQ2VyMDdDYnBIQT09IiwibWFjIjoiNWJmMGY3NjA5NzFjNzMxMmQ0NjRjMGYxZTU5OTYyYTRhZDg3YmYxYTI3ZTZlNTNlODdjN2I0OGVkYTczYmVmOSJ9; _gid=GA1.2.1791915242.1578567999; __rtgt_sid=k56mo37fac8qav; d7s_spc=1; _awl=2.1578568073.0.4-2292a19e-520ac2c588eca8bc42c8f1549c858300-6763652d617369612d6561737431-5e170989-0; fitracking_2=yes; fiTrackingDomainParams=%7B%22host%22%3A%22tracking1.firstimpression.io%22%2C%22type%22%3A%22full%22%7D; fi_utm=direct%7Cdirect%7C%7C%7C%7C; _pk_ses.6495.73a4=*; XSRF-TOKEN=eyJpdiI6ImFoSnkrZXhibGZFRlFrRGkrRU55dEE9PSIsInZhbHVlIjoiakgrWkt5ZWs5cGYxOXMwK21Sdk4wbEhEZUVhYmFpQjBLRTZLOUREMTBuYklmcDRwZ1JqSzRtZDFTRHJVTGFwSCIsIm1hYyI6ImY4NTc5NjI2ZTU4NDBiMWE3MTE5YmViZWE0YTg1NjI4MGNiYTNiNWFlN2NhYTVkNDg0NTEzMGE2Njg2MTMxZWMifQ%3D%3D; laravel_session=eyJpdiI6IjNvOWE1V08yK2ZtQlNNRzlSdDlwaEE9PSIsInZhbHVlIjoicHRFNGhXMzhYUDBaV1NiY3dMVFwvR1wvU1NURlpRWE1qb3NpUzdEbEdrb0VJckZ4eDRvN0hjck43RlF1aHJoTm44IiwibWFjIjoiODMxZjlkOTJkZDJhMmMyNThiMWZlYjEzOGVkMjc1MjliYzQ1YmYyYmRkZTJkY2MxNThkNjY2ODJmMWM3ZGY1NyJ9; _pk_id.6495.73a4=468ed0dbee279aee.1578568080.1.1578568134.1578568080.; IC_ViewCounter_www.barchart.com=3; bcPromoPremierShowed=true; _gat_UA-2009749-51=1',
            'referer':'https://www.barchart.com/stocks/quotes/{}/options/expiration={}',
            'sec-fetch-mode':'cors',
            'sec-fetch-site':'same-origin',
            'x-xsrf-token': 'eyJpdiI6ImFoSnkrZXhibGZFRlFrRGkrRU55dEE9PSIsInZhbHVlIjoiakgrWkt5ZWs5cGYxOXMwK21Sdk4wbEhEZUVhYmFpQjBLRTZLOUREMTBuYklmcDRwZ1JqSzRtZDFTRHJVTGFwSCIsIm1hYyI6ImY4NTc5NjI2ZTU4NDBiMWE3MTE5YmViZWE0YTg1NjI4MGNiYTNiNWFlN2NhYTVkNDg0NTEzMGE2Njg2MTMxZWMifQ==',
            'User-Agent':user_agent
        }

        tasks = []
        async with aiohttp.ClientSession() as session:
            for expiry in expirys:
                headers['referer'] = headers['referer'].format(symbol,expiry)
                headers[':path'] = headers[':path'].format(symbol,expiry)
                task = asyncio.ensure_future(self._xp_fetch(symbol,expiry,url,session,headers))
                tasks.append(task)
            # gather returns responses in original order not arrival order
            #   https://docs.python.org/3/library/asyncio-task.html#task-functions
            responses = await asyncio.gather(*tasks)
            return responses

# =========================获取last price============================
class Last_price_scraper:
    def __init__(self):
        pass

    async def _fetch(self,symbol,url,session):
        '''fn: to retrieve option quotes as JSON
        Params:
            symbol : str(), ETF
            url : str(), request url
            session : aiohttp.ClientSession() object
        Returns:
            response : text object
        '''
        async with session.get(url.format(symbol)) as response:
            return await response.text()

    async def run(self,symbols):
        '''fn: to aggregate response option quotes
        Params:
            symbols : list of str(), ETF symbols
        Returns:
            responses : list of text
        '''
        url = 'https://www.barchart.com/stocks/quotes/{}/options'

        tasks = []
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                task = asyncio.ensure_future(self._fetch(symbol,url,session))
                tasks.append(task)
               # gather returns responses in original order not arrival order
            #   https://docs.python.org/3/library/asyncio-task.html#task-functions
            responses = await asyncio.gather(*tasks)
            return responses