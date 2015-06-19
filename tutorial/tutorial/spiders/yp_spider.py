import scrapy
from tutorial.items import YpItem
import urlparse
from scrapy.http import Request

class DmozSpider(scrapy.Spider):
    name = "yp"
    allowed_domains = ["yellowpages.com"]
    download_delay = 2
    counter = 0

    #TODO: change this to use a category and city name and build the url
    # start_urls = [
    # "http://www.yellowpages.com/search?search_terms=cupcakes&geo_location_terms=Tucson%2C+AZ"
    # ]
    start_urls = ["http://www.yellowpages.com/tucson-az/cupcake-stores"]

    def parse_business(self, response):
        self.log("downloading business info")
        global counter
        item = YpItem()

        # id: how do we get the id? from base url maybe like above?
        # response.xpath('//div[@id="mip-impression"][@data-analytics]')[0].extract().split(',')

        # this returns [u'Mon - Sat', u'7:00 am - 6:00 pm', u'Sun', u'8:00 am - 2:00 pm'] -- what to do
        item['hours'] = response.xpath('//dd[@class="open-hours"]//div[@class="open-details"]//p//time[@datetime]//text()').extract() 
        item.general_info = " ".join(info for info in response.xpath('//dd[@class="description"]//text()').extract())
        item['services'] = response.xpath('//dd//ul/li//text()').extract() # list for this?
        item.payment_method = response.xpath('//dd[@class="payment"]//text()').extract() # list for this?
        item.location = " ".join(loc for loc in response.xpath('//dd[@class="location-description"]//text()').extract())
        item.aka = response.xpath('//dd[@class="aka"]//p/text()').extract()
        item.other_link = response.xpath('//dd[@class="weblinks"]//a[@href]//text()').extract()
        item.categories = response.xpath('//dd[@class="categories"]//a/text()').extract()
        item.neighborhood = response.xpath('//dd[@class="neighborhoods"]//a/text()')
        street = " ".join(street for street in response.xpath('//div[@class="contact"]/p[@class="street-address"]/text()').extract())
        city = "".join(city for city in response.xpath('//div[@class="contact"]/p[@class="city-state"]/text()').extract())
        item.contact = street + city
        counter += 1
        if counter == 10:
            self.log("got to 10 items")
            raise CloseSpider('stopping for testing')
        return [item]

    def parse(self, response):
        #base_url =  response.url.split("/search")[-2]
        base_url = "//".join(response.url.split("/")[:3])
        requests = []
        for business in response.xpath('//h3[@class="n"]'):
            url = business.xpath('a/@href')[0].extract()
            self.log(url)
            if url[0] != "/": # if it doesn't start with '/' ignore -- external link
                self.log("FAKE")
                continue
            else:
                self.log("parsing business")
                requests.append(Request(url=urlparse.urljoin(base_url, url), callback=self.parse_business))
                self.log("formed request")
        return requests







