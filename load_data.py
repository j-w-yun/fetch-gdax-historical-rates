from datetime import datetime, timedelta
import time

import gdax
from matplotlib import pyplot as plt
from simplejson import JSONDecodeError

import numpy as np


def fetch_data(symbol, col, start, end, granularity, filename):
    """Fetch GDAX historic rates.
    Args:
        symbol (str): Cryptocurrency/fiat pair
        col (int): Column to fetch data from. (3) is opening price
        start (datetime): Start of historic data
        end (datetime): End of historic data
        granularity (int): Delta time, in minutes
    """
    try:
        r = np.load(filename)
    except IOError:
        r = []
        delta = timedelta(minutes=granularity * 100)

        slice_start = start
        while slice_start != end:
            slice_end = min(slice_start + delta, end)
            slice = __request_slice(symbol,
                                    __date_to_iso8601(slice_start),
                                    __date_to_iso8601(slice_end),
                                    granularity=granularity * 60)
            slice_start = slice_end

            for elem in reversed(slice):
                r.append(elem[col])

        r = np.array(r)
        np.save(filename, r)

    return r.astype(float)


def __request_slice(symbol, start, end, granularity):
    client = gdax.PublicClient()
    retries = 20
    for retry_count in range(retries):
        try:
            r = client.get_historic_rates(symbol, start, end, granularity)
        except JSONDecodeError:
            print('JSONDecodeError | retrying')
            continue

        if 'message' in r and r['message'] == 'Rate limit exceeded':
            print(r)
            time.sleep(1.5 ** retry_count)
        else:
            print('date:', start, '| fetch successful | retries:', retry_count)
            return r


def __date_to_iso8601(date):
    """Convert date to ISO-8601 format
    """
    return '{year}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}'.format(
        year=date.year,
        month=date.month,
        day=date.day,
        hour=date.hour,
        minute=date.minute,
        second=date.second)


def plot_prices(train_prices, test_prices=None):
    plt.xlabel('time')
    plt.ylabel('price')
    plt.plot(range(len(train_prices)), train_prices, 'b')
    if test_prices is not None:
        plt.plot(range(len(train_prices), len(train_prices) + len(test_prices)), test_prices, 'g')
    plt.show()


if __name__ == "__main__":
    r = fetch_data("ETH-USD", 3, datetime(2016, 6, 1), datetime(2018, 1, 25), 5, "crypto_prices.npy")
    print(r)
    plot_prices(r)
