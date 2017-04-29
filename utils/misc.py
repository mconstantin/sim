from datetime import datetime


def datetime_as_string(dt):
    if isinstance(dt, datetime):
        return datetime.strftime(dt, '%m/%d/%y %H:%M')
    else:
        return dt


def get_device_with_most_data(devices):
    maxlen = 0
    device = None
    values = None
    type_index = -1
    color_index = -1
    for d in devices.values():
        (ti, ci), vals = d.get_longest_counter_serie()
        if vals and len(vals) > maxlen:
            maxlen = len(vals)
            device = d
            type_index = ti
            color_index = ci
            values = vals

    if device:
        print('device SN={}, counter with index={}, color={}, has {} values:\n{}'
              .format(device.serial_number, type_index, color_index, len(values),
                      '\n'.join(['%s at %s' % (val, datetime_as_string(when)) for when, val in values])))
    return device
