from array import array
from datetime import datetime, timedelta
import struct

MAX_QUOTES_PER_MESSAGE = 50
MICROS_PER_SECOND = 1_000_000


def serialize_address(host: str, port: int) -> bytes:
    """

    :param host:
    :param port:
    :return: Byte version of host and port
    """
    host_byte = []
    host_part = host.split(".")

    for item in host_part:
        num = int(item)
        host_byte.append(num)

    host_byte = bytearray(host_byte)
    port_byte = port.to_bytes(2, 'big')

    return host_byte + port_byte


def deserialize_price(price_bytes: bytes) -> float:
    price_float = struct.unpack('d', price_bytes)
    return price_float[0]


def deserialize_utcdatetime(utc_serialize: bytes) -> datetime:
    """

    :param utc_serialize:
    :return: 8-byte stream
    """
    epoch = datetime(1970, 1, 1)
    p = array('Q')

    p.frombytes(utc_serialize)
    p.byteswap()  # to big-endian

    utc_serialize = p[0] / MICROS_PER_SECOND
    utc = timedelta(seconds=utc_serialize) + epoch

    return utc


def deserialize_price(b: bytes) -> float:
    x = struct.unpack('d', b)

    return x[0]


def unmarshal_message(message_marshal: bytes) -> list:
    if len(message_marshal) > MAX_QUOTES_PER_MESSAGE * 32:
        raise ValueError('max quotes exceeded for a single message')

    timeToLoop = len(message_marshal) / 32

    dict_list = []

    for entry in range(0, int(timeToLoop)):
        c = message_marshal[0:32]
        date = c[0:8]
        dictionary = {}
        dictionary['timestamp'] = deserialize_utcdatetime(date)

        cross1 = c[8:11]
        dictionary['cross1'] = cross1.decode('utf-8')

        cross2 = c[11:14]
        dictionary['cross2'] = cross2.decode('utf-8')

        price = c[14:22]
        dictionary['price'] = deserialize_price(price)

        message_marshal = message_marshal[32:]

        dict_list.append(dict(dictionary))

    return dict_list