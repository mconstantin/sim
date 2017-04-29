import paho.mqtt.client as mqtt
from uuid import getnode as get_mac


def on_connect(client, publisher, rc):
    if rc == 0:
        assert publisher.mqttc == client
        print('client {} connected successfully'.format(client._client_id))
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


def on_disconnect(client, publisher, rc):
    if rc == 0:
        assert publisher.mqttc == client
        print('client {} disconnected successfully'.format(client._client_id))
    else:
        print('client disconnected unexpectedly')


def on_publish(client, publisher, mid):
    # assert publisher.mqttc == client
    # print('message {} published successfully'.format(mid))
    pass


class Publisher:
    def __init__(self):
        mac = get_mac()
        client_id = str(hex(mac))[2:] + '-pub'
        self.mqttc = mqtt.Client(client_id=client_id, userdata=self)
        self.mqttc.on_connect = on_connect
        self.mqttc.on_disconnect = on_disconnect
        self.mqttc.on_publish = on_publish

    def __enter__(self):
        self.mqttc.connect('iot.eclipse.org')
        self.mqttc.loop_start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.mqttc.disconnect()
        self.mqttc.loop_stop()
        if exc_type is not None:
            print('{}: {} exception at client disconnect'.format(str(exc_type),
                                                                 ', '.join([str(arg) for arg in exc_val.args])))
        return True

    def send(self, topic, payload):
        '''
        Publish the message with this payload at this topic.
        :param topic: a tuple of strings, e.g. ('device', '204567', 'MX-2600 N')
                      The topic create will be 'device/204567|MX-2600N'
        :param payload: a dictionary, e.g. {'job-counter-type-index': 11, 'job-color-type-index': 2, 'counter: 800}
                        the payload (as string) will be 'job-counter-type-index=11|job-color-type-index=2|counter=800'
        :return: True if success, False otherwise
        '''

        parent = topic[0]
        child = '|'.join(topic[1:]) if len(topic) > 1 else ''
        topic = parent + '/' + child
        data = ['%s=%s' % (key.replace('_', '-'), val) for key, val in payload.items()]
        msg = None
        if data:
            msg = '|'.join(data)
        try:
            if msg:
                print("message '{}' on topic '{}'".format(msg if msg else '', topic))
            res = self.mqttc.publish(topic, payload=msg.encode('utf-8'))
            return res[0] == mqtt.MQTT_ERR_SUCCESS
        except ValueError as e:
            print('publish error: {}'.format(str(e)))
            return False

