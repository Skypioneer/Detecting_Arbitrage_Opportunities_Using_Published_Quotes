import socket
from self import self
import fxp_bytes_subscriber
import bellman_ford
self.dict = {}


def if_contain_same_cross_exchange(cross1, cross2, price):
    key = cross1 + cross2

    if key in self.dict:
        old_price = self.dict[key]
        self.dict[key] = price
        return old_price
    else:
        self.dict[key] = price


class Subscriber(object):
    server_addr = ('localhost', 0)
    print('starting up on {} port {}', format(server_addr))

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as socket:
        socket.bind(server_addr)
        addr = socket.getsockname()
        print('Address is {}'.format(addr))
        message = fxp_bytes_subscriber.serialize_address(addr[0], addr[1])
        socket.sendto(message, ('localhost', 50403))
        print('blocking, waiting for message...\n')

        while True:
            data = socket.recv(4096)

            message = fxp_bytes_subscriber.unmarshal_message(data)

            for entry in message:
                curr_time = entry['timestamp']
                cross1 = entry['cross1']
                cross2 = entry['cross2']
                price = entry['price']
                if_contain_same_cross_exchange(cross1, cross2, price)
                print(f'{curr_time} {cross1} {cross2} {price}')

            bellman_f = bellman_ford.Test(message)
            profit_list = bellman_f.bellman_ford('USD')

            if profit_list is not None:
                index = 0
                price = 100
                list = profit_list
                print("ARBITRAGE:")
                print('\t\tstart with USD $100')

                for _ in list:
                    if index + 1 == len(list): break
                    rate = bellman_f.get_rate(list[index], list[index+1])
                    price = price * rate
                    print(f'\t\texchange {list[index]} for {list[index+1]} at {rate} --> {list[index + 1]} {price}')
                    index += 1

            print()


if __name__ == '__main__':
    subscriber = Subscriber()
