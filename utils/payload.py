from datetime import datetime, date, time


def dunder(name):
    return name.startswith('__') and name.endswith('__')


class Payload:
    WILDCARDS = {'+': 'PLUS', '#': 'POUND'}
    payload_attribs = tuple()

    def get_payload(self):
        payload = dict()
        for attr_name in self.payload_attribs:
            if not hasattr(self, attr_name):
                print('invalid payload attribute: {}'.format(attr_name))
                continue
            attr = getattr(self, attr_name, None)
            if attr is None:
                payload[attr_name] = ''
                continue
            if hasattr(attr, 'get_payload'):
                payload.update(attr.get_payload())
            elif isinstance(attr, datetime):
                payload[attr_name] = datetime.strftime(attr, '%x %H:%M')
            elif isinstance(attr, date):
                payload[attr_name] = date.strftime(attr, '%x')
            elif isinstance(attr, time):
                payload[attr_name] = date.strftime(attr, '%H:%M')
            else:
                payload[attr_name] = str(attr)

        return payload

    @classmethod
    def from_payload(cls, payload):
        o = cls()
        for attr_name in dir(o):
            if dunder(attr_name):
                continue
            val = None
            key = attr_name.replace('_', '-')
            attr = getattr(o, attr_name, None)
            if hasattr(attr, 'from_payload'):
                val = attr.from_payload(payload)
            elif key in payload:
                val = payload[key]
                if attr_name == 'serial_number' or key == 'device_model':
                    for wc, wc_repl in Payload.WILDCARDS.items():
                        val = val.replace(wc_repl, wc)
            if val is not None:
                setattr(o, attr_name, val)

        return o
