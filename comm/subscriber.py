import paho.mqtt.client as mqtt
from uuid import getnode as get_mac

from devices.mfps import Device
from storage import Repository
from utils.misc import get_device_with_most_data


def on_connect(client, subscriber, rc):
    if rc == 0:
        assert subscriber.mqttc == client
        print('client {} connected successfully'.format(client._client_id))
        res1, subscriber.mid_registration = client.subscribe(subscriber.TOPIC_MFP_REGISTRATION)
        if res1 != mqtt.MQTT_ERR_SUCCESS:
            print('failed to subscribe to the {} topic'.format(subscriber.TOPIC_MFP_REGISTRATION))
        res2, subscriber.mid_data = client.subscribe(subscriber.TOPIC_MFP_DATA)
        if res2 != mqtt.MQTT_ERR_SUCCESS:
            print('failed to subscribe to the {} topic'.format(subscriber.TOPIC_MFP_DATA))
    elif rc == 1:
        print('connection refused: incorrect protocol version')
    elif rc == 2:
        print('connection refused: invalid client identifier')
    elif rc == 3:
        print('connection refused: server unavailable')
    elif rc == 4:
        print('connection refused: bad username or password')
    elif rc == 5:
        print('connection refused: not authorized')
    else:
        print('connection refused: should have not been used')


def on_disconnect(client, subscriber, rc):
    if rc == 0:
        assert subscriber.mqttc == client
        print('client {} disconnected successfully'.format(client._client_id))
    else:
        print('client disconnected unexpectedly')


def on_subscribe(client, subscriber, mid, granted_qos):
    assert subscriber.mqttc == client
    print('client subscribed successfully with message id={} and QoS {}'
          .format(mid, ', '.join([str(qos) for qos in granted_qos])))


def on_unsubscribe(client, subscriber, mid):
    assert subscriber.mqttc == client
    print('client unsubscribed successfully with message id={}'.format(mid))


def on_message(client, subscriber, message):
    print("Received message '" + str(message.payload.decode('utf-8')) + "' on topic '"
          + message.topic + "' with QoS " + str(message.qos))

    payload = dict()
    if message.topic == Subscriber.TOPIC_MFP_REGISTRATION:
        pairs = message.payload.decode('utf-8').split('|')
        for pair in pairs:
            key, val = pair.split('=')
            payload[key] = val

        device = Device.from_payload(payload)
        subscriber.add_device(device)
    elif message.topic.startswith(Subscriber.TOPIC_MFP_DATA_PREFIX):
        pairs = message.payload.decode('utf-8').split('|')
        for pair in pairs:
            key, val = pair.split('=')
            payload[key] = val

        topic, subtopic = message.topic.split('/')
        sn, model = subtopic.split('|')
        device = subscriber.get_device((sn, model))
        if device:
            device.add_data(payload['date-added'], payload['job-counter-type-index'], payload['job-color-type-index'],
                            payload['count'])
    else:
        print('unsubscribed message "{}" received'.format(message.topic))


class Subscriber:
    TOPIC_MFP_REGISTRATION = 'mfp-register/'
    TOPIC_MFP_DATA = 'mfp/+'
    TOPIC_MFP_DATA_PREFIX = 'mfp/'

    def __init__(self):
        mac = get_mac()
        client_id = str(hex(mac))[2:] + '-sub'
        self.mqttc = mqtt.Client(client_id=client_id, userdata=self)
        self.mqttc.connect('iot.eclipse.org')
        self.mqttc.on_connect = on_connect
        self.mqttc.on_disconnect = on_disconnect
        self.mqttc.on_subscribe = on_subscribe
        self.mqttc.on_unsubscribe = on_unsubscribe
        self.mqttc.on_message = on_message
        self.repo = Repository()

    def add_device(self, device):
        self.repo.devices[(device.serial_number, device.device_model)] = device

    def get_device(self, key):
        return self.repo.devices.get(key, None)

    def get_num_devices(self):
        return len(self.repo.devices)

    def __enter__(self):
        self.mqttc.connect('iot.eclipse.org')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.mqttc.unsubscribe(self.TOPIC_MFP_REGISTRATION)
        self.mqttc.unsubscribe(self.TOPIC_MFP_DATA)
        self.mqttc.disconnect()
        if exc_type is not None:
            print('{}: {} exception at client disconnect'.format(str(exc_type),
                                                                 ', '.join([str(arg) for arg in exc_val.args])))
        return True

    def start(self):
        self.mqttc.loop_forever()

    def stop(self):
        self.mqttc.unsubscribe(self.TOPIC_MFP_REGISTRATION)
        self.mqttc.unsubscribe(self.TOPIC_MFP_DATA)
        self.mqttc.disconnect()


if __name__ == '__main__':
    with Subscriber() as sub:
        try:
            sub.start()
        except KeyboardInterrupt:
            pass

    print('got {} devices'.format(sub.get_num_devices()))
    # find the device with the longest counter trail
    get_device_with_most_data(sub.repo.devices)

