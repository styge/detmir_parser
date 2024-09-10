from datetime import datetime
from urllib.parse import urlparse

import scrapy


class DetmirApiSpider(scrapy.Spider):
    name = 'detmir'
    api_url = 'https://api.detmir.ru/v2/products/new'

    start_urls = [
        'https://www.detmir.ru/catalog/index/name/bombery_vo/',
        'https://www.detmir.ru/catalog/index/name/bryuki_uteplennye/'
    ]

    limit = 36  # максимальное количество товаров на 1-й странице

    def start_requests(self):
        for url in self.start_urls:
            parsed_url = urlparse(url)
            category = parsed_url.path.split('/')[-2]
            params = {
                'filter': f'categories[].alias:{category};platform:web;promo:false;site:detmir;withregion:RU-MOW',
                'expand': 'meta.filter.info,meta.filters.delivery_speed,webp',
                'exclude': 'stores',
                'limit': self.limit,
                'sort': 'popularity:desc',
                'platform': 'web',
                'offset': 0
            }

            api_url = f'{self.api_url}?{self._params_to_query_string(params)}'
            yield scrapy.Request(url=api_url, callback=self.parse, meta={'category': category, 'offset': 0})

    def parse(self, response):
        products = response.json()

        for product in products:
            rpc = product['id']
            product_url = product['link']['web_url']
            title = product['title']
            brand = product['brands'][0]['title']

            marketing_tags = []
            for label in product['labels']:
                if label['name']:
                    marketing_tags.append(label['name'])

            price_data = {
                'current': product['prices']['sale'],
                'original': product['prices']['old'],
            }
            if price_data['current'] != price_data['original'] and \
                    type(price_data['current']) == type(price_data['original']):
                price_data['sale_tag'] = f'Скидка {product["discount_percentage"]}%'

            stock = {}
            if product['available']['online']['warehouse_codes']:
                stock['in_stock'] = True
            else:
                stock['in_stock'] = False

            assets = {'main_image': product['pictures'][0]['original'], 'set_images': []}
            for image in product['pictures']:
                assets['set_images'].append(image['original'])
            assets['video'] = []
            if product['videos']:
                assets['video'] = product['videos'][0]['url']

            description = product['description']
            description_selector = scrapy.Selector(text=description)
            description = description_selector.xpath('//text()').get().replace('\r\n', '')

            article = product['article']
            sex = product['sex']
            rating = product['rating']
            review_count = product['review_count']
            questions_count = product['questions_count']

            vendor_id = product['vendor']['code']
            vendor_name = product['vendor']['name']
            vendor_inn = product['vendor']['inn']
            vendor_ogrn = product['vendor']['ogrn']
            vendor_phone = product['vendor']['phone']
            vendor_address = product['vendor']['address']

            yield {
                'timestamp': datetime.now(),
                'RPC': rpc,
                'url': product_url,
                'title': title,
                'marketing_tags': marketing_tags,
                'brand': brand,
                'price_data': price_data,
                'stock': stock,
                'assets': assets,
                'metadata': {
                    '__description': description,
                    'АРТИКУЛ': article,
                    'пол': sex,
                    'рейтинг': rating,
                    'кол-во отзывов': review_count,
                    'кол-во вопросов': questions_count,
                    'vendor_info': {
                        'id': vendor_id,
                        'name': vendor_name,
                        'inn': vendor_inn,
                        'ogrn': vendor_ogrn,
                        'phone': vendor_phone,
                        'address': vendor_address
                    }
                }
            }

        if len(products) == self.limit:
            offset = response.meta['offset'] + self.limit
            category = response.meta['category']

            params = {
                'filter': f'categories[].alias:{category};platform:web;promo:false;site:detmir;withregion:RU-MOW',
                'expand': 'meta.filter.info,meta.filters.delivery_speed,webp',
                'exclude': 'stores',
                'limit': self.limit,
                'sort': 'popularity:desc',
                'platform': 'web',
                'offset': offset
            }

            next_page_url = f"{self.api_url}?{self._params_to_query_string(params)}"
            yield scrapy.Request(url=next_page_url, callback=self.parse, meta={'category': category, 'offset': offset})

    def _params_to_query_string(self, params):
        """Функция для формирования query string из параметров"""
        return '&'.join([f'{key}={value}' for key, value in params.items()])
