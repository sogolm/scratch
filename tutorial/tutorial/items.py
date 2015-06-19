import scrapy


class YpItem(scrapy.Item):
    ypid = scrapy.Field()
    name = scrapy.Field()
    hours = scrapy.Field()
    general_info = scrapy.Field()
    services = scrapy.Field()
    payment_method = scrapy.Field()
    location = scrapy.Field()
    neighborhood = scrapy.Field()
    aka = scrapy.Field()
    other_link = scrapy.Field()
    categories = scrapy.Field()
    neighborhood = scrapy.Field()
    contact = scrapy.Field()
