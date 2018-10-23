import pandas as pd
import requests

from .exceptions import PaginationError
from functools import wraps
from socket import gaierror
from time import sleep

from dateutil import rrule
from itertools import tee


def year_blocks(start, end):
    """
    Create pairs of start and end with max a year in between, 
    to deal with usage restrictions on the API

    Parameters
    ----------
    start : dt.datetime | pd.Timestamp
    end : dt.datetime | pd.Timestamp

    Returns
    -------
    ((pd.Timestamp, pd.Timestamp))
    """
    rule = rrule.YEARLY

    res = [pd.Timestamp(day)
           for day in rrule.rrule(rule, dtstart=start, until=end)]
    res.append(end)
    res = sorted(set(res))
    res = pairwise(res)
    return res


def day_blocks(start, end):
    """
    Create pairs of start and end with max a day in between, 
    to deal with usage restrictions on the API

    Parameters
    ----------
    start : dt.datetime | pd.Timestamp
    end : dt.datetime | pd.Timestamp

    Returns
    -------
    ((pd.Timestamp, pd.Timestamp))
    """
    rule = rrule.DAILY

    res = [pd.Timestamp(day)
           for day in rrule.rrule(rule, dtstart=start, until=end)]
    res.append(end)
    res = sorted(set(res))
    res = pairwise(res)
    return res


def pairwise(iterable):
    """
    Create pairs to iterate over
    eg. [A, B, C, D] -> ([A, B], [B, C], [C, D])

    Parameters
    ----------
    iterable : iterable

    Returns
    -------
    iterable
    """
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def retry(func):
    """Catches connection errors, waits and retries"""
    @wraps(func)
    def retry_wrapper(*args, **kwargs):
        self = args[0]
        error = None
        for _ in range(self.retry_count):
            try:
                result = func(*args, **kwargs)
            except (requests.ConnectionError, gaierror) as e:
                error = e
                print("Connection Error, retrying in {} seconds".format(
                    self.retry_delay))
                sleep(self.retry_delay)
                continue
            else:
                return result
        else:
            raise error
    return retry_wrapper


def paginated(func):
    """Catches a PaginationError, splits the requested period in two and tries
    again. Finally it concatenates the results"""

    @wraps(func)
    def pagination_wrapper(*args, **kwargs):
        try:
            df = func(*args, **kwargs)
        except PaginationError:
            start = kwargs.pop('start')
            end = kwargs.pop('end')
            pivot = start + (end - start) / 2
            df1 = pagination_wrapper(*args, start=start, end=pivot, **kwargs)
            df2 = pagination_wrapper(*args, start=pivot, end=end, **kwargs)
            df = pd.concat([df1, df2])
        return df

    return pagination_wrapper


def year_limited(func):
    """Deals with calls where you cannot query more than a year, by splitting
    the call up in blocks per year"""

    @wraps(func)
    def year_wrapper(*args, **kwargs):
        start = kwargs.pop('start')
        end = kwargs.pop('end')
        blocks = year_blocks(start, end)
        frames = [func(*args, start=_start, end=_end, **kwargs)
                  for _start, _end in blocks]
        df = pd.concat(frames)
        return df

    return year_wrapper


def day_limited(func):
    """Deals with calls where you cannot query more than a day, by splitting
    the call up in blocks per day"""

    @wraps(func)
    def day_wrapper(*args, **kwargs):
        start = kwargs.pop('start')
        end = kwargs.pop('end')
        blocks = day_blocks(start, end)
        frames = [func(*args, start=_start, end=_end, **kwargs)
                  for _start, _end in blocks]
        df = pd.concat(frames)
        return df

    return day_wrapper
