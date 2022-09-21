import scrapy
import requests
import sys
import datetime
import csv
from solar.items import SolarItem
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http import FormRequest
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.response import open_in_browser

def generate_dates(start_date,end_date):
    dates_in_between = [start_date + datetime.timedelta(days=x) for x in range(0, (end_date-start_date).days+1)]
    return dates_in_between

class solarSpider(scrapy.Spider):
    def __init__(self, start=None, end=None, meters="", *args, **kwargs):
        super(solarSpider, self).__init__(*args, **kwargs)
        try:
            self.start_date = datetime.datetime.strptime(start, "%m%d%Y")
        except (AttributeError, NameError, TypeError):
            self.start_date = datetime.datetime.strptime("01012018", "%m%d%Y")

        try:
            self.end_date = datetime.datetime.strptime(end, "%m%d%Y")
        except (AttributeError, NameError, TypeError):
            self.end_date = datetime.datetime.today()

        try:
            self.meters = [meter.strip() for meter in meters.split(',')]
        except Exception as e:
            print(e)

    name = "solar"
    allowed_domains = []
    start_urls = ["http://ets.aeso.ca/ets_web/docroot/Market/Reports/HistoricalReportsStart.html"]
    rules = ()

    def parse(self, response):
        dates = generate_dates(self.start_date, self.end_date)

        headers = {
            'Host': 'ets.aeso.ca',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.42 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Referer': 'http://ets.aeso.ca/ets_web/ip/IPHistoricalReportsServlet',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ru,en-US;q=0.9,en;q=0.8,ht;q=0.7,ja;q=0.6'
        }

        for date in dates:
            request = scrapy.Request(url='http://ets.aeso.ca/ets_web/ip/Market/Reports/PublicSummaryAllReportServlet?beginDate='+
                    date.strftime("%m%d%Y")+'&endDate='+date.strftime("%m%d%Y")+'&contentType=csv',
                    headers=headers,dont_filter=True,callback=self.parse_meters)
            request.meta['date'] = date
            yield request

    def parse_meters(self, response):
        meta = response.meta
        date = meta['date']
        parsed_meters = {}
        response_lines = response.text.splitlines()
        for line in response_lines:
            for meter in self.meters:
                if meter in line:
                    csv_string = list(csv.reader([line]))[0]
                    parsed_meters[meter] = csv_string[3:]
        meta['meters'] = parsed_meters

        headers = {
            'Host': 'ets.aeso.ca',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.42 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Referer': 'http://ets.aeso.ca/ets_web/ip/IPHistoricalReportsServlet',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ru,en-US;q=0.9,en;q=0.8,ht;q=0.7,ja;q=0.6'
        }
        request = scrapy.Request(url='http://ets.aeso.ca/ets_web/ip/Market/Reports/HistoricalPoolPriceReportServlet?beginDate='+
                date.strftime("%m%d%Y")+'&endDate='+date.strftime("%m%d%Y")+'&contentType=csv', 
                headers=headers, meta=meta, dont_filter=True, callback=self.parse_price)
        yield request

    def parse_price(self, response):
        meta = response.meta
        date = meta['date']
        price = {}
        response_lines = response.text.splitlines()
        for line in response_lines:
            if date.strftime("%m/%d/%Y") in line:
                csv_string = list(csv.reader([line]))[0]
                price[csv_string[0].split(' ')[1]] = csv_string[1:]
        meta['price'] = price

        headers = {
            'Host': 'climate.weather.gc.ca',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.42 Safari/537.36',
            'DNT': '1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ru,en-US;q=0.9,en;q=0.8,ht;q=0.7,ja;q=0.6'
        }

        if date < datetime.datetime(2012, 7, 12):
            station_id = "2205"
        else:
            station_id = "50430"

        request = scrapy.Request('http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID='+station_id+'&Year='+
            str(date.year)+'&Month='+str(date.month)+'&timeframe=1', 
            headers=headers, meta=meta, dont_filter=True, callback=self.parse_calgary_weather)
        yield request

    def parse_calgary_weather(self,response):
        meta = response.meta
        date = meta['date']
        calgary_weather_conditions = {}
        response_lines = response.text.splitlines()
        for line in response_lines:
            if date.strftime("%Y-%m-%d") in line:
                csv_string = list(csv.reader([line]))[0]
                if date.strftime("%Y-%m-%d") in csv_string[0]:
                    calgary_weather_conditions[csv_string[0].split(' ')[1]] = csv_string[5:]
        meta['calgary_weather_conditions'] = calgary_weather_conditions
        
        headers = {
            'Host': 'climate.weather.gc.ca',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.42 Safari/537.36',
            'DNT': '1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ru,en-US;q=0.9,en;q=0.8,ht;q=0.7,ja;q=0.6'
        }

        if date < datetime.datetime(2012, 4, 12):
            station_id = "1865"
        else:
            station_id = "50149"

        request = scrapy.Request('http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID='+station_id+'&Year='+
            str(date.year)+'&Month='+str(date.month)+'&timeframe=1', 
            headers=headers, meta=meta, dont_filter=True, callback=self.parse_edmonton_weather)
        yield request

    def parse_edmonton_weather(self, response):
        meta = response.meta
        date = meta['date']
        edmonton_weather_conditions = {}
        response_lines = response.text.splitlines()
        for line in response_lines:
            if date.strftime("%Y-%m-%d") in line:
                csv_string = list(csv.reader([line]))[0]
                if date.strftime("%Y-%m-%d") in csv_string[0]:
                    edmonton_weather_conditions[csv_string[0].split(' ')[1]] = csv_string[5:]
        meta['edmonton_weather_conditions'] = edmonton_weather_conditions
        
        headers = {
            'Host': 'climate.weather.gc.ca',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.42 Safari/537.36',
            'DNT': '1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ru,en-US;q=0.9,en;q=0.8,ht;q=0.7,ja;q=0.6'
        }

        if date < datetime.datetime(2008, 9, 25):
            station_id = "2519"
        elif date < datetime.datetime(2011, 10, 20):
            station_id = "31288"
        else:
            station_id = "49490"

        request = scrapy.Request('http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID='+station_id+'&Year='+
            str(date.year)+'&Month='+str(date.month)+'&timeframe=1', 
            headers=headers, meta=meta, dont_filter=True, callback=self.parse_mcmurray_weather)
        yield request

    def parse_mcmurray_weather(self, response):
        meta = response.meta
        date = meta['date']
        mcmurray_weather_conditions = {}
        response_lines = response.text.splitlines()
        for line in response_lines:
            if date.strftime("%Y-%m-%d") in line:
                csv_string = list(csv.reader([line]))[0]
                if date.strftime("%Y-%m-%d") in csv_string[0]:
                    mcmurray_weather_conditions[csv_string[0].split(' ')[1]] = csv_string[5:]
        meta['mcmurray_weather_conditions'] = mcmurray_weather_conditions
        
        for hour in range(24):
            item = SolarItem()

            item['Date'] = date.strftime("%Y-%m-%d")
            item['Hour'] = str(hour)
            item['Meters'] = {k : v[hour:hour+1][0] if v[hour:hour+1] else '' for k,v in meta.get('meters',[]).items()}
            item['Pool_price'] = meta.get('price',{}).get(str(hour+1).zfill(2),[])[0:1] or ''
            item['Calgary_temp'] = meta.get('calgary_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[0:1] or ''
            item['Calgary_dew_point_temp'] = meta.get('calgary_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[2:3] or ''
            item['Calgary_rel_hum'] = meta.get('calgary_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[4:5] or ''
            item['Calgary_wind_dir'] = meta.get('calgary_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[6:7] or ''
            item['Calgary_wind_spd'] = meta.get('calgary_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[8:9] or '' 
            item['Calgary_visibility'] = meta.get('calgary_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[10:11] or ''
            item['Calgary_stn_press'] = meta.get('calgary_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[12:13] or ''
            item['Calgary_hmdx'] = meta.get('calgary_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[14:15] or ''
            item['Calgary_wind_chill'] = meta.get('calgary_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[16:17] or ''
            item['Calgary_weather'] = meta.get('calgary_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[18:19] or ''
            item['Edmonton_temp'] = meta.get('edmonton_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[0:1] or ''
            item['Edmonton_dew_point_temp'] = meta.get('edmonton_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[2:3] or ''
            item['Edmonton_rel_hum'] = meta.get('edmonton_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[4:5] or ''
            item['Edmonton_wind_dir'] = meta.get('edmonton_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[6:7] or ''
            item['Edmonton_wind_spd'] = meta.get('edmonton_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[8:9] or ''
            item['Edmonton_visibility'] = meta.get('edmonton_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[10:11] or ''
            item['Edmonton_stn_press'] = meta.get('edmonton_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[12:13] or ''
            item['Edmonton_hmdx'] = meta.get('edmonton_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[14:15] or ''
            item['Edmonton_wind_chill'] = meta.get('edmonton_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[16:17] or ''
            item['Edmonton_weather'] = meta.get('edmonton_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[18:19] or ''
            item['McMurray_temp'] = meta.get('mcmurray_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[0:1] or ''
            item['McMurray_dew_point_temp'] = meta.get('mcmurray_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[2:3] or ''
            item['McMurray_rel_hum'] = meta.get('mcmurray_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[4:5] or ''
            item['McMurray_wind_dir'] = meta.get('mcmurray_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[6:7] or ''
            item['McMurray_wind_spd'] = meta.get('mcmurray_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[8:9] or ''
            item['McMurray_visibility'] = meta.get('mcmurray_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[10:11] or ''
            item['McMurray_stn_press'] = meta.get('mcmurray_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[12:13] or ''
            item['McMurray_hmdx'] = meta.get('mcmurray_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[14:15] or ''
            item['McMurray_wind_chill'] = meta.get('mcmurray_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[16:17] or ''
            item['McMurray_weather'] = meta.get('mcmurray_weather_conditions',{}).get(str(hour).zfill(2)+':00',[])[18:19] or ''

            yield item