from collections import defaultdict
from datetime import datetime, date
from utils.payload import Payload


NULL = 'NULL'


class Product(Payload):
    payload_attribs = ('product_name', 'product_family')

    def __init__(self, family=None, name=None):
        self.product_name = name
        self.product_family = family


class Owner(Payload):
    payload_attribs = ('dealer_name', 'dealer_id', 'location', 'purchase_date', 'registration_date', 'summary',
                       'dealer_website', 'owner_id', 'user_name', 'helper_id')

    def __init__(self, row=None):
        self.dealer_name = None
        self.location = None
        self.purchase_date = None
        self.registration_date = None
        self.summary = None
        self.dealer_website = None
        self.dealer_id = None
        self.helper_id = None
        self.owner_id = None
        self.user_name = None

        if row:
            self.dealer_name = row['Dealer Name']
            self.location = row['Location']
            try:
                self.purchase_date = datetime.strptime(row['PurchaseDate'], '%m/%d/%y') \
                    if row['PurchaseDate'] != NULL else None
            except ValueError:
                self.purchase_date = None
                print('\npurchase date error: %s' % row['PurchaseDate'])
            try:
                self.registration_date = datetime.strptime(row['RegistrationDate'], '%m/%d/%y') \
                    if row['RegistrationDate'] != NULL else None
            except ValueError:
                self.registration_date = None
                print('\nregister date error: %s' % row['RegistrationDate'])
            self.summary = row['Summary']
            self.dealer_website = row['Dealer Homepage']
            self.dealer_id = row['DealershipID']
            self.helper_id = row['MfpHelperId']
            self.owner_id = row['OwnerID']
            self.user_name = row['User Name']


class Host(Payload):
    payload_attribs = ('host', 'domain_name', 'ip', 'lanip', 'port')

    def __init__(self, row=None):
        self.host = None
        self.domain_name = None
        self.ip = None
        self.lanip = None
        self.port = None

        if row:
            self.host = row['HostName']
            self.domain_name = row['DomainName']
            self.ip = row['IPAddress']
            self.lanip = row['LANIPAddress']
            self.port = row['LocalPort']


class Counters:
    def __init__(self):
        self.counters = defaultdict(list)
        self.last_key_added = None

    def __call__(self, *args, **kwargs):
        when = args[0]
        type_index = args[1]
        color_index = args[2]
        value = args[3]
        if (type_index, color_index) in self.counters:
            last_when, last_value = self.counters[(type_index, color_index)][-1]
            if when == last_when:
                return False
            if value == last_value:
                return False

        self.counters[(type_index, color_index)].append((when, value))
        self.last_key_added = (type_index, color_index)
        return True

    def get_longest_serie(self):
        return max(list(self.counters.items()), key=lambda x: len(x[1]), default=((0, 0), list()))

    def get_last_payload(self):
        when, val = self.counters[self.last_key_added][-1]
        payload = {
            'job_counter_type_index': self.last_key_added[0],
            'job_color_type_index': self.last_key_added[1],
            'date_added': datetime.strftime(when, '%x %H:%M'),
            'count': str(val)
        }
        return payload


class Device(Payload):
    payload_attribs = ('serial_number', 'device_model', 'device_type', 'altername_model_name', 'product', 'owner',
                       'host', 'machine_code', 'app_ver', 'description')

    TOPIC_MFP_REGISTRATION = 'mfp-register'
    TOPIC_MFP_DATA = 'mfp'

    def __init__(self, row=None):
        self.counters = Counters()
        self.serial_number = None
        self.device_type = None
        self.device_model = None
        self.altername_model_name = None
        self.product = Product()
        self.owner = Owner()
        self.host = Host()
        self.machine_code = None
        self.app_ver = None
        self.description = None

        if row:
            self.serial_number = row['SerialNumber']
            self.device_type = row['Product Type']
            self.device_model = row['Model Name']
            self.altername_model_name = row['AlternateModelName']
            self.product = Product(row['Product Family Name'], row['Product Name'])
            self.owner = Owner(row)
            self.host = Host(row)
            self.machine_code = row['MachineCode']
            self.app_ver = row['ApplicationVersion']
            self.description = row['Description']

    def add_data(self, *args, **kwargs):
        return self.counters(*args, **kwargs)

    def send_message(self, pub, row):
        if self.add_data(datetime.strptime(row['DateAdded'], '%x %H:%M'),
                         row['JobCounterTypeIndex'], row['JobColorTypeIndex'], row['Count']):
            # print('counter=%s' % row['Count'])
            serial_number = self.serial_number
            device_model = self.device_model
            for wc, wc_repl in self.WILDCARDS.items():
                serial_number = serial_number.replace(wc, wc_repl)
                device_model = device_model.replace(wc, wc_repl)
            res = pub.send((self.TOPIC_MFP_DATA, serial_number, device_model), self.counters.get_last_payload())
            if not res:
                print('send device (sn={}, model={}) message error'.format(self.serial_number, self.device_model))

    def register_device(self, pub):
        res = pub.send((self.TOPIC_MFP_REGISTRATION,), self.get_payload())
        if not res:
            print('register device (sn={}, model={}) message error'.format(self.serial_number, self.device_model))

    def get_longest_counter_serie(self):
        return self.counters.get_longest_serie()