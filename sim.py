import csv
import sys

from comm.publisher import Publisher
from devices.mfps import Device
from utils.misc import get_device_with_most_data


class Simulator:
    def __init__(self, fn):
        self.fn = fn
        self.devices = {}

    def load(self, count=sys.maxsize):
        dots = 0
        with open(self.fn, 'r') as csvfile, Publisher() as pub:
            reader = csv.DictReader(csvfile)
            for row in reader:
                count -= 1
                if count <= 0:
                    break
                dots += 1
                if dots % 1000 == 0:
                    print('.', end='', flush=True)
                # the key should be (serial_number, model_name)
                key = (row['SerialNumber'], row['Model Name'])
                device = self.devices.get(key, None)
                if device:
                    device.send_message(pub, row)
                else:
                    device = Device(row)
                    self.devices[key] = device
                    device.register_device(pub)


if __name__ == '__main__':
    sim = Simulator(sys.argv[1])
    sim.load(count=1000)
    print('', flush=True)
    print('number of devices=%d' % len(sim.devices))
    # find the device with the longest counter trail
    get_device_with_most_data(sim.devices)

