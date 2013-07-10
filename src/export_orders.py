'''
Created on Jul 5, 2013

@author: zentrung
'''


from datetime import datetime, timedelta
from logging import Formatter, getLogger, StreamHandler, FileHandler
from math import ceil
from time import sleep

import requests
import sys
import time
import unicodecsv


SHOP_URL = 'your_store.myshopify.com'
API_KEY = ''
PASSWORD = ''
SHARED_SECRET = ''


DATETIME_FORMAT = '%Y-%m-%d %H:%M'
DATE_FORMAT = '%Y-%m-%d'
ORDERS_PER_PAGE_LIMIT = 250.0 # In float
API_CALLS_RESET_DURATION = 300 # In seconds
STEP = timedelta(hours=12) # Fetch orders every 12 hours
DISCOUNT_CODE_DELIMITER = '|'
CSV_DELIMITER = ','
CSV_QUOTECHAR = '"'

LOG_LEVEL = 'DEBUG'

ADDRESS_FIELDS = ('province', 'city', 'first_name', 'last_name', 'name', 'zip',
        'province_code', 'address1', 'address2', 'longitude', 'phone',
        'country_code', 'country', 'latitude', 'company')
FULFILLMENT_FIELDS = ('status', 'line_items', 'receipt', 'service', 'order_id',
        'created_at', 'tracking_urls', 'updated_at', 'tracking_url',
        'tracking_number', 'tracking_numbers', 'tracking_company', 'id')
LINEITEM_FIELDS = ('sku', 'properties', 'vendor', 'product_id', 'title',
        'price', 'requires_shipping', 'name', 'fulfillment_status',
        'variant_inventory_management', 'fulfillment_service', 'variant_id',
        'variant_title', 'quantity', 'id', 'product_exists', 'grams')
ORDER_FIELDS = ('subtotal_price', 'buyer_accepts_marketing', 'reference',
        'shipping_lines', 'cart_token', 'number', 'taxes_included', 'currency',
        'total_weight', 'closed_at', 'cancel_reason', 'location_id', 'gateway',
        'confirmed', 'user_id', 'fulfillments', 'landing_site',
        'total_price_usd', 'financial_status', 'id', 'note', 'source',
        'processing_method', 'total_line_items_price', 'cancelled_at', 'test',
        'email', 'total_tax', 'billing_address', 'checkout_token', 'tax_lines',
        'landing_site_ref', 'updated_at', 'total_discounts', 'discount_codes',
        'checkout_id', 'customer', 'browser_ip', 'referring_site', 'line_items',
        'total_price', 'name', 'client_details', 'created_at', 'note_attributes',
        'fulfillment_status', 'token', 'shipping_address', 'order_number')
SHIPPINGLINE_FIELDS = ('source', 'price', 'code', 'title')
TAXLINE_FIELDS = ('price', 'rate', 'title')
OUTPUT_CSV_FIELDS = (
    'Name',
    'Email',
    'Financial Status',
    'Paid at', # UNKNOWN CORRESPONDENCE
    'Fulfillment Status',
    'Fulfilled at', # ['fulfillments'][index]
    'Accepts Marketing',
    'Currency',
    'Subtotal',
    'Shipping', # ['shipping_lines'][index]
    'Taxes',
    'Total',
    'Discount Code',
    'Discount Amount',
    'Shipping Method', # ['shipping_lines'][index]
    'Created at',
    'Lineitem quantity', # ['line_items'][index]
    'Lineitem name', # ['line_items'][index]
    'Lineitem price', # ['line_items'][index]
    'Lineitem compare at price', # UNKNOWN CORRESPONDENCE - Always True
    'Lineitem sku', # ['line_items'][index]
    'Lineitem requires shipping', # ['line_items'][index]
    'Lineitem taxable', # UNKNOWN CORRESPONDENCE - Always True
    'Lineitem fulfillment status', # ['line_items'][index]
    'Billing Name', # ['billing_address']
    'Billing Street', # To be constructed (``address1``[,``address2``])
    'Billing Address1', # ['billing_address']
    'Billing Address2', # ['billing_address']
    'Billing Company', # ['billing_address']
    'Billing City', # ['billing_address']
    'Billing Zip', # ['billing_address']
    'Billing Province', # ['billing_address']
    'Billing Country', # ['billing_address']
    'Billing Phone', # ['billing_address']
    'Shipping Name', # ['shipping_address']
    'Shipping Street', # To be constructed (``address1``[,``address2``])
    'Shipping Address1', # ['shipping_address']
    'Shipping Address2', # ['shipping_address']
    'Shipping Company', # ['shipping_address']
    'Shipping City', # ['shipping_address']
    'Shipping Zip', # ['shipping_address']
    'Shipping Province', # ['shipping_address']
    'Shipping Country', # ['shipping_address']
    'Shipping Phone', # ['shipping_address']
    'Notes',
    'Note Attributes',
    'CA State Tax', # ['tax_lines'][index] where ``title`` = 'CA State Tax'
    'Cancelled at',
    'Payment Method',
    'Payment Reference', # UNKNOWN CORRESPONDENCE
    'Refunded Amount', # UNKNOWN CORRESPONDENCE
    'Vendor' # ['line_items'][index]
)
'''
# Fields with JSON fields as key and CSV headers as value
FIELDS_MAPPER = {
    'name': 'Name',
    'email': 'Email',
    'financial_status': 'Financial Status',
    '__field_missed_1': 'Paid at',
    'fulfillment_status': 'Fulfillment Status',
    'created_at': 'Fulfilled at', # ['fulfillments'][index]
    'buyer_accepts_marketing': 'Accepts Marketing',
    'currency': 'Currency',
    'subtotal_price': 'Subtotal',
    'price': 'Shipping', # ['shipping_lines'][index]
    'total_tax': 'Taxes',
    'total_price': 'Total',
    'discount_codes': 'Discount Code',
    'total_discounts': 'Discount Amount',
    'title': 'Shipping Method', # ['shipping_lines'][index]
    'created_at': 'Created at',
    'quantity': 'Lineitem quantity', # ['line_items'][index]
    'name': 'Lineitem name', # ['line_items'][index]
    'price': 'Lineitem price', # ['line_items'][index]
    '__field_missed_2': 'Lineitem compare at price', # Always empty
    'sku': 'Lineitem sku', # ['line_items'][index]
    'requires_shipping': 'Lineitem requires shipping', # ['line_items'][index]
    '__field_missed_3': 'Lineitem taxable', # Always True
    'fulfillment_status': 'Lineitem fulfillment status', # ['line_items'][index]
    'name': 'Billing Name', # ['billing_address']
    '__field_missed_4': 'Billing Street', # To be constructed (``address1``[,``address2``])
    'address1': 'Billing Address1', # ['billing_address']
    'address2': 'Billing Address2', # ['billing_address']
    'company': 'Billing Company', # ['billing_address']
    'city': 'Billing City', # ['billing_address']
    'zip': 'Billing Zip', # ['billing_address']
    'province_code': 'Billing Province', # ['billing_address']
    'country_code': 'Billing Country', # ['billing_address']
    'phone': 'Billing Phone', # ['billing_address']
    'name': 'Shipping Name', # ['shipping_address']
    '__field_missed_5': 'Shipping Street', # To be constructed (``address1``[,``address2``])
    'address1': 'Shipping Address1', # ['shipping_address']
    'address2': 'Shipping Address2', # ['shipping_address']
    'company': 'Shipping Company', # ['shipping_address']
    'city': 'Shipping City', # ['shipping_address']
    'zip': 'Shipping Zip', # ['shipping_address']
    'province_code': 'Shipping Province', # ['shipping_address']
    'country_code': 'Shipping Country', # ['shipping_address']
    'phone': 'Shipping Phone', # ['shipping_address']
    'note': 'Notes',
    'note_attributes': 'Note Attributes',
    'price': 'CA State Tax', # ['tax_lines'][index] where ``title`` = 'CA State Tax'
    'cancelled_at': 'Cancelled at',
    'gateway': 'Payment Method',
    '__field_missed_6': 'Payment Reference',
    '__field_missed_7': 'Refunded Amount',
    'vendor': 'Vendor' # ['line_items'][index]
}
'''


def __validateJsonFields(json_payload, fields_tuple):
    keys = json_payload.keys()
    for field in fields_tuple:
        if field not in keys:
            return False
    return True

def __request(request, url, params):
    try:
        import simplejson as json
    except ImportError:
        import json
    r = request.get(url, params=params)
    logger.d('url {}'.format(r.url))
    logger.d('response {}'.format(r.content))
    return json.loads(r.content)


class MyLogger():
    '''
    Standard logging.py wrapper
    '''
    def __init__(self, name):
        self.logger = getLogger(name)
        self.logger.setLevel(LOG_LEVEL)
        

        # Format logger records
        self.formatter= Formatter('%(asctime)s %(name)s [%(levelname)s]: %(message)s')

        # Setup console logging
        ch = StreamHandler()
        ch.setLevel(LOG_LEVEL)
        ch.setFormatter(self.formatter)
        self.logger.addHandler(ch)

        # Setup file logging
        d = datetime.now()
        log_file = 'export_orders_{}.log'.format(datetime.strftime(d, '%y%m%d_%H%M%S'))
        fh = FileHandler(log_file)
        fh.setLevel(LOG_LEVEL)
        fh.setFormatter(self.formatter)
        self.logger.addHandler(fh)

    def c(self, message):
        self.logger.critical(message)

    def d(self, message):
        self.logger.debug(message)

    def e(self, message):
        self.logger.error(message)

    def i(self, message):
        self.logger.info(message)

    def w(self, message):
        self.logger.warn(message)

class InvalidPayloadException(Exception): pass

class Address():
    def __init__(self, json_payload):
        def __validateJsonFields(json_payload, fields_tuple):
            keys = json_payload.keys()
            for field in fields_tuple:
                if field not in keys:
                    return False
            return True
        if __validateJsonFields(json_payload, ADDRESS_FIELDS):
            self.json_fields = json_payload
        else:
            raise InvalidPayloadException
        self.street = self.__getStreet()

    def __getStreet(self):
        address1 = self.json_fields.get('address1')
        address2 = self.json_fields.get('address2')
        street = address1 or ''
        street += ', ' + address2 if address2 else ''
        return street

    def get(self, key):
        if key == 'street':
            return self.street
        else:
            return self.json_fields.get(key, '')

class Fulfillment():
    def __init__(self, json_payload):
        def __validateJsonFields(json_payload, fields_tuple):
            keys = json_payload.keys()
            for field in fields_tuple:
                if field not in keys:
                    return False
            return True
        if __validateJsonFields(json_payload, FULFILLMENT_FIELDS):
            self.json_fields = json_payload
            self.line_items = [LineItem(_) for _ in json_payload.get('line_items')]
        else:
            raise InvalidPayloadException

    def get(self, key):
        return self.json_fields.get(key, '')

class LineItem():
    def __init__(self, json_payload):
        def __validateJsonFields(json_payload, fields_tuple):
            keys = json_payload.keys()
            for field in fields_tuple:
                if field not in keys:
                    return False
            return True
        if __validateJsonFields(json_payload, LINEITEM_FIELDS):
            self.json_fields = json_payload
        else:
            raise InvalidPayloadException

    def get(self, key):
        return self.json_fields.get(key, '')

class Order():
    def __init__(self, json_payload):
        def __validateJsonFields(json_payload, fields_tuple):
            keys = json_payload.keys()
            for field in fields_tuple:
                if field not in keys:
                    return False
            return True
        if __validateJsonFields(json_payload, ORDER_FIELDS):
            self.json_fields = json_payload
            self.line_items = [LineItem(_) for _ in json_payload.get('line_items')]
            self.shipping_lines = [ShippingLine(_) for _ in json_payload.get('shipping_lines')]
            self.tax_lines = [TaxLine(_) for _ in json_payload.get('tax_lines')]
            self.billing_address = Address(json_payload.get('billing_address'))
            self.shipping_address = Address(json_payload.get('shipping_address'))
            self.fulfillments = [Fulfillment(_) for _ in json_payload.get('fulfillments')]
        else:
            raise InvalidPayloadException
        # Discover Anomalies
        if len(self.shipping_lines) > 1:
            logger.w('discover_anomaly {}: More than 1 shipping_lines detected'.format(self.get('name')))
        if len(self.tax_lines) > 1:
            logger.w('discover_anomaly {}: More than 1 tax_lines detected'.format(self.get('name')))
        if len(self.fulfillments) > 1:
            logger.w('discover_anomaly {}: More than 1 fulfillments detected'.format(self.get('name')))

    def get(self, key):
        return self.json_fields.get(key, '')

    def generateCsvRows(self):
        line_items_count = len(self.line_items)
        rows = []
        # Generate first row with complete data
        discount_codes = ''
        for discount_code in self.get('discount_codes'):
            if discount_code.get('code'):
                discount_codes += DISCOUNT_CODE_DELIMITER + discount_code.get('code')
        discount_codes = discount_codes[1:]
        rows.append((
            self.get('name'),
            self.get('email'),
            self.get('financial_status'),
            '',
            self.get('fulfillment_status'),
            self.fulfillments[0].get('created_at'),
            'yes' if self.get('buyer_accepts_marketing') else 'no',
            self.get('currency'),
            self.get('subtotal_price'),
            self.shipping_lines[0].get('price'),
            self.get('total_tax'),
            self.get('total_price'),
            discount_codes,
            self.get('total_discounts'),
            self.shipping_lines[0].get('title'),
            self.get('created_at'),
            self.line_items[0].get('quantity'),
            self.line_items[0].get('name'),
            self.line_items[0].get('price'),
            '',
            self.line_items[0].get('sku'),
            self.line_items[0].get('requires_shipping'),
            'true',
            self.line_items[0].get('fulfillment_status'),
            self.billing_address.get('name'),
            self.billing_address.get('street'),
            self.billing_address.get('address1'),
            self.billing_address.get('address2'),
            self.billing_address.get('company'),
            self.billing_address.get('city'),
            self.billing_address.get('zip'),
            self.billing_address.get('province_code'),
            self.billing_address.get('country_code'),
            self.billing_address.get('phone'),
            self.shipping_address.get('name'),
            self.shipping_address.get('street'),
            self.shipping_address.get('address1'),
            self.shipping_address.get('address2'),
            self.shipping_address.get('company'),
            self.shipping_address.get('city'),
            self.shipping_address.get('zip'),
            self.shipping_address.get('province_code'),
            self.shipping_address.get('country_code'),
            self.shipping_address.get('phone'),
            self.get('note'),
            self.get('note_attributes') if self.get('note_attributes') else '',
            self.tax_lines[0].get('price') if self.tax_lines and self.tax_lines[0].get('title') == 'CA State Tax' else '',
            self.get('cancelled_at'),
            self.get('gateway'),
            '',
            '',
            self.line_items[0].get('vendor')
        ))
        # Generate 1 extra row per LineItem
        for i in range(1, line_items_count):
            rows.append((
                self.get('name'),
                self.get('email'),
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                self.get('created_at'),
                self.line_items[i].get('quantity'),
                self.line_items[i].get('name'),
                self.line_items[i].get('price'),
                '',
                self.line_items[i].get('sku'),
                self.line_items[i].get('requires_shipping'),
                'true',
                self.line_items[i].get('fulfillment_status'),
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                self.line_items[i].get('vendor')
            ))
        return rows

class ShippingLine():
    def __init__(self, json_payload):
        def __validateJsonFields(json_payload, fields_tuple):
            keys = json_payload.keys()
            for field in fields_tuple:
                if field not in keys:
                    return False
            return True
        if __validateJsonFields(json_payload, SHIPPINGLINE_FIELDS):
            self.json_fields = json_payload
        else:
            raise InvalidPayloadException

    def get(self, key):
        return self.json_fields.get(key, '')

class TaxLine():
    def __init__(self, json_payload):
        def __validateJsonFields(json_payload, fields_tuple):
            keys = json_payload.keys()
            for field in fields_tuple:
                if field not in keys:
                    return False
            return True
        if __validateJsonFields(json_payload, TAXLINE_FIELDS):
            self.json_fields = json_payload
        else:
            raise InvalidPayloadException

    def get(self, key):
        return self.json_fields.get(key, '')

logger = MyLogger('export_orders')


def getOrdersCount(request, date_min, date_max):
    params = {
        'updated_at_min': date_min.strftime(DATETIME_FORMAT),
        'updated_at_max': date_max.strftime(DATETIME_FORMAT)
    }
    url = 'https://{}/admin/orders/count.json'.format(SHOP_URL)
    result = __request(request, url, params)
    order_count = result.get('count')
    logger.d('order_count {}'.format(order_count))
    return order_count

def fetchOrders(request, date_min, date_max, page=1):
    params = {
        'updated_at_min': date_min.strftime(DATETIME_FORMAT),
        'updated_at_max': date_max.strftime(DATETIME_FORMAT),
        'page': page,
        'limit': int(ORDERS_PER_PAGE_LIMIT)
    }
    url = 'https://{}/admin/orders.json'.format(SHOP_URL)
    result = __request(request, url, params)
    orders_json = result.get('orders')
    orders_list = []
    if orders_json:
        for order_json in orders_json:
            orders_list.append(Order(order_json))
    logger.d('order_list {}'.format(orders_list))
    return orders_list

def sendEmail():
    pass

def fetchOrdersToCsv(request, beginning_date, ending_date, csv_filename):
    '''
    Fetch all orders from ``beginning_date`` to ``ending_date``
    ``beginning_date`` and ``ending_date`` format: YYYY-MM-DD
    '''
    beginning_date = datetime.strptime(beginning_date, DATE_FORMAT)
    ending_date = datetime.strptime(ending_date, DATE_FORMAT)

    date_min = beginning_date
    date_max = beginning_date + STEP
    with open(csv_filename, 'a') as csv_file:
        # Write CSV header
        dict_writer = unicodecsv.DictWriter(csv_file, delimiter=CSV_DELIMITER, fieldnames=OUTPUT_CSV_FIELDS)
        dict_writer.writeheader()
        # Write data
        csv_writer = unicodecsv.writer(csv_file, delimiter=CSV_DELIMITER, quotechar=CSV_QUOTECHAR, encoding='utf-8')
        while date_max <= ending_date:
            orders_count = getOrdersCount(request, date_min, date_max)
            page_count = int(ceil(orders_count/ORDERS_PER_PAGE_LIMIT))
            for page in range(1, page_count+1):
                order_list = fetchOrders(request, date_min, date_max, page)
    #                 time.sleep(API_CALLS_RESET_DURATION)
                for order in order_list:
                    csv_writer.writerows(order.generateCsvRows())
            date_min = date_max
            date_max += STEP

def main(options):
    request = requests.Session(auth=(API_KEY, PASSWORD))
    fetchOrdersToCsv(request, '2013-07-08', '2013-07-09', 'test.csv')

if __name__ == '__main__':
    main(sys.argv[:1])