# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class SolarItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    Date = scrapy.Field()
    Hour = scrapy.Field()
    Meters = scrapy.Field()
    Pool_price = scrapy.Field()
    Calgary_temp = scrapy.Field()
    Calgary_dew_point_temp = scrapy.Field()
    Calgary_rel_hum = scrapy.Field()
    Calgary_wind_dir = scrapy.Field()
    Calgary_wind_spd = scrapy.Field()
    Calgary_visibility = scrapy.Field()
    Calgary_stn_press = scrapy.Field()
    Calgary_hmdx = scrapy.Field()
    Calgary_wind_chill = scrapy.Field()
    Calgary_weather = scrapy.Field()
    Edmonton_temp = scrapy.Field()
    Edmonton_dew_point_temp = scrapy.Field()
    Edmonton_rel_hum = scrapy.Field()
    Edmonton_wind_dir = scrapy.Field()
    Edmonton_wind_spd = scrapy.Field()
    Edmonton_visibility = scrapy.Field()
    Edmonton_stn_press = scrapy.Field()
    Edmonton_hmdx = scrapy.Field()
    Edmonton_wind_chill = scrapy.Field()
    Edmonton_weather = scrapy.Field()
    McMurray_temp = scrapy.Field()
    McMurray_dew_point_temp = scrapy.Field()
    McMurray_rel_hum = scrapy.Field()
    McMurray_wind_dir = scrapy.Field()
    McMurray_wind_spd = scrapy.Field()
    McMurray_visibility = scrapy.Field()
    McMurray_stn_press = scrapy.Field()
    McMurray_hmdx = scrapy.Field()
    McMurray_wind_chill = scrapy.Field()
    McMurray_weather = scrapy.Field()
