import scrapy
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from cars import AutoscoutCar


urls_to_scrape = []

class BroadSpider(scrapy.Spider):

    name = "Base_Spider"
    base_url = "https://www.autoscout24.de"

    """
    Autoscount links look like this:  https://www.autoscout24.de/ergebnisse?mmvmk0=9&mmvmd0=1634&mmvco=1&cy=D&powertype=kw&atype=C&ustate=N%2CU&sort=standard&desc=0
    where mmvmk0 represents the brand and mmvmd represents the model.
    
    This scraper scrapes the site wide and collects all links required for the deep scraping.
    """

    brands = {
        'AUDI': 9,
        'BMW': 13,
        'FORD': 29,
        'Mercedes': 47,
        'Opel': 54,
        'Renault': 60,
        'Volkswagen': 74,
    }
    Audi_Modelle = {
        '100': 1619,
        '200': 1620,
        '50': 16581,
        '80': 1622,
        '90': 1623,
        'A1': 19083,
        'A2': 16416,
        'A3': 1624,
        'A4': 1626,
        'A4 Allroad': 20493,
        'A5': 19047,
        'A6': 1628,
        'A6 allroad': 20494,
        'A7': 19216,
        'A8': 1629,
        'Allroad': 16414,
        'Cabriolet': 1630,
        'Coupet': 1631,
        'Q2': 74373,
        'Q3': 19715,
        'Q5': 19155,
        'Q7': 18683,
        'Q8': 74734,
        'Quattro': 18000,
        'R8': 18925,
        'RS': 15735,
        'RS Q3': 20385,
        'RS2': 19728,
        'RS3': 19729,
        'RS4': 19730,
        'RS5': 19731,
        'RS6': 19732,
        'RS7': 20295,
        'RS7 Sportsback': 20317,
        'S1': 20369,
        'S2': 2109,
        'S3': 15637,
        'S4': 2108,
        'S5': 19048,
        'S6': 1633,
        'S7': 20155,
        'S8': 15123,
        'SQ2': 74735,
        'SQ5': 20164,
        'SQ7': 74439,
        'TT': 15627,
        'TT RS': 20056,
        'TTS': 20055,
        'V8': 1634
    }
    BMW_Modelle = {
        '114': 20149,
        '116': 18480,
        '118': 18481,
        '120': 18482,
        '123': 19078,
        '125': 19084,
        '130': 18588,
        '135': 19079,
        '140': 74386,
        '2002': 2163,
        '214': 20958,
        '216': 20397,
        '218': 20327,
        '220': 20326,
        '225': 20328,
        '228': 20481,
        '230': 74387,
        '235': 20329,
        '240': 74388,
        '315': 16558,
        '316': 1639,
        '318': 1640,
        '320': 1641,
        '323': 1642,
        '324': 1643,
        '325': 1644,
        '328': 1645,
        '330': 15779,
        '335': 18805,
        '340': 21097,
        'Active Hybrid 3': 20187,
        '418': 20398,
        '420': 20289,
        '425': 20399,
        '428': 20287,
        '430': 20330,
        '435': 20288,
        '440': 74335,
        '518': 1647,
        '520': 1648,
        '523': 1649,
        '524': 1650,
        '525': 1651,
        '528': 2145,
        '530': 1652,
        '535': 1653,
        '540': 1654,
        '545': 18383,
        '550': 18710,
        'Active Hybrid 5': 20027,
        '628': 15780,
        '630': 18491,
        '633': 16436,
        '635': 1656,
        '640': 19714,
        '645': 18400,
        '650': 18709,
        '725': 1657,
        '728': 1658,
        '730': 1659,
        '732': 15953,
        '735': 1660,
        '740': 1661,
        '745': 16598,
        '750': 1662,
        '760': 18327,
        'Active Hybrid 7': 19541,
        '830': 21100,
        '840': 1663,
        '850': 1664,
        'i3': 20319,
        'i8': 20320,
        '1er M coupe': 19741,
        'M2': 21157,
        'M3': 1646,
        'M4': 20393,
        'M5': 1655,
        'M6': 18577,
        'M1': 1667,
        'X1': 19242,
        'X2': 74769,
        'X3': 18387,
        'X4': 20364,
        'X4M': 74322,
        'X5': 16406,
        'X5M': 20174,
        'X6': 19110,
        'X6M': 20175,
        'Z1': 1666,
        'Z2': 1665,
        'Z3M': 20223,
        'Z4': 18308,
        'Z4M': 19617,
        'Z8': 16402,
        'Misc': 2107384
    }

    def _get_links(self):

        """
        Create our subset for all our Models and Brands.
        :return:
        """

        BaseUrl = 'https://www.autoscout24.de/ergebnisse?sort=standard&desc=0&ustate=N%2CU&cy=D&atype=C'
        Audi = []
        BMW = []

        for key, value in self.Audi_Modelle.items():
            _tmp_url = BaseUrl + '&mmvmk0=%s' % str(self.brands["AUDI"]) + '&mmvmd0=%s' % str(value)
            for x in range(1, 20):
                Audi.append(_tmp_url + '&page=%s' % str(x))


        for key, value in self.BMW_Modelle.items():
            _tmp_url = BaseUrl + '&mmvmk0=%s' % str(self.brands["BMW"]) + '&mmvmd0=%s' % str(value)
            for x in range(1, 20):
                BMW.append(_tmp_url + '&page=%s' % str(x))

        return Audi, BMW

    def start_requests(self):

        Audi, BMW = self._get_links()
        for url in Audi:
            yield scrapy.Request(url=url, callback=self.parse)

        for url in BMW:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        all_links = response.css('div.cldt-summary-titles').extract()
        for x in all_links:
            global urls_to_scrape
            urls_to_scrape.append(self.base_url + x.split(" ", 4)[3][6:-7])

    def __init__(self):
        self.start_requests()
        print("Hallo")


class DeepSpider(scrapy.Spider):

    tmp_var = 0

    def __init__(self):
        pass

    def start_requests(self):
        for url in urls_to_scrape:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, request):
        tst = AutoscoutCar(request)


@defer.inlineCallbacks
def main():
    configure_logging()
    runner = CrawlerRunner()
    yield runner.crawl(BroadSpider)
    yield runner.crawl(DeepSpider)
    reactor.stop()


if __name__ == '__main__':
    main()
    reactor.run()
