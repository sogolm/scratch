import scrapy
from tutorial.items import YpItem
import urlparse
from scrapy.http import Request
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors import LinkExtractor
import json


class YpSpider(CrawlSpider):
    name = "yp"
    allowed_domains = ["yellowpages.com"]
    download_delay = 2
    counter = 0

    #TODO: change this to use a category and city name and build the url
    # start_urls = [
    # "http://www.yellowpages.com/search?search_terms=cupcakes&geo_location_terms=Tucson%2C+AZ"
    # ]
    start_urls = ["http://www.yellowpages.com/tucson-az/cupcake-stores?page="]
    rules = [Rule(LinkExtractor(allow=['\d+']), 'parse_search')]

    #use Link extractor from CrawlSpider to move between pages.

    def parse_search(self, response):
        #TODO: FIX THIS LINK TO BE UNHACKY
        base_url = "http://" + response.url.split("/")[-3] 
        requests = []
        for business in response.xpath('//h3[@class="n"]'):
            url = business.xpath('a/@href')[0].extract()
            if url[0] != "/": # if it doesn't start with '/' ignore -- external link #TODO: IS THERE A BETTER WAY TO HANDLE THIS?
                continue
            else:
                url = urlparse.urljoin(base_url, url)
                requests.append(Request(url=url, callback=self.parse_business))
        return requests

    def parse_business(self, response):
        self.log("downloading business info")
        item = YpItem()
        data_model = response.xpath('//a[@href="#addBusiness"]/@data-model').extract()[0]
        data_model_dict = json.loads(data_model)
        item['name'] = data_model_dict['name']
        item['ypid'] = data_model_dict['ypid']
        item['hours'] = response.xpath('//dd[@class="open-hours"]//div[@class="open-details"]//p//time[@datetime]//text()').extract() 
        item['general_info'] = " ".join(info for info in response.xpath('//dd[@class="description"]//text()').extract())
        item['services'] = response.xpath('//dd//ul/li//text()').extract()
        item['payment_method'] = response.xpath('//dd[@class="payment"]//text()').extract()
        item['location'] = " ".join(loc for loc in response.xpath('//dd[@class="location-description"]//text()').extract())
        item['aka'] = response.xpath('//dd[@class="aka"]//p/text()').extract()
        item['other_link'] = response.xpath('//dd[@class="weblinks"]//a[@href]//text()').extract()
        item['categories'] = response.xpath('//dd[@class="categories"]//a/text()').extract()
        item['neighborhood'] = response.xpath('//dd[@class="neighborhoods"]//a/text()').extract()
        street = " ".join(street for street in response.xpath('//div[@class="contact"]/p[@class="street-address"]/text()').extract())
        city = "".join(city for city in response.xpath('//div[@class="contact"]/p[@class="city-state"]/text()').extract())
        item['contact'] = street + city
        self.counter += 1
        if self.counter == 10:
            self.log("got to 10 items")
            raise scrapy.exceptions.CloseSpider('stopping for testing')
        self.log("done downloading %d items" %self.counter)
        return item
