# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import csv
import datetime
from scrapy import signals
from scrapy.exporters import CsvItemExporter
from operator import itemgetter
from ast import literal_eval

start_date = datetime.datetime.today()
end_date = datetime.datetime.today()
meters = []

class CSVPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):    
        file = open('output.csv', 'w+b')
        self.meters = spider.meters
        self.files[spider] = file
        self.exporter = CsvItemExporter(file)
        self.exporter.fields_to_export = [
                'Date',
                'Hour',
                'Meters',
                'Pool_price',
                'Calgary_temp',
                'Calgary_dew_point_temp',
                'Calgary_rel_hum',
                'Calgary_wind_dir',
                'Calgary_wind_spd',
                'Calgary_visibility',
                'Calgary_stn_press',
                'Calgary_hmdx',
                'Calgary_wind_chill',
                'Calgary_weather',
                'Edmonton_temp',
                'Edmonton_dew_point_temp',
                'Edmonton_rel_hum',
                'Edmonton_wind_dir',
                'Edmonton_wind_spd',
                'Edmonton_visibility',
                'Edmonton_stn_press',
                'Edmonton_hmdx',
                'Edmonton_wind_chill',
                'Edmonton_weather',
                'McMurray_temp',
                'McMurray_dew_point_temp',
                'McMurray_rel_hum',
                'McMurray_wind_dir',
                'McMurray_wind_spd',
                'McMurray_visibility',
                'McMurray_stn_press',
                'McMurray_hmdx',
                'McMurray_wind_chill',
                'McMurray_weather'
            ]
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files.pop(spider)
        file.close()

        with open('output.csv', 'r') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            original_list = list(reader)
            cleaned_list = list(filter(None,original_list))
            sorted_list = sorted(cleaned_list, key=lambda row: (row[0], int(row[1])))
            del(headers[2])
            headers[2:2] = self.meters

            with open('output_sorted.csv', 'w', newline='') as output_file:
                wr = csv.writer(output_file, dialect='excel')
                wr.writerow(headers)
                for data in sorted_list:
                    if data[2]:
                        meter_values = [literal_eval(data[2]).get(meter,'') for meter in self.meters]
                        del(data[2])
                        data[2:2] = meter_values
                        wr.writerow(data)

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

class SolarPipeline(object):
    def process_item(self, item, spider):
        return item