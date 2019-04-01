from functools import wraps
from threading import Thread


def on_deposit_failure(**kwargs):
    print(
        '\nDEPOSIT failure!!!!',
        kwargs.get('username', None),
        str(kwargs.get('amount', None))
    )


def on_withdraw_failure(**kwargs):
    print(
        '\nWITHDRAW failure!!!!',
        kwargs.get('username', None),
        str(kwargs.get('amount', None))
    )


def threadedtask(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        t = Thread(target=fn, *args, **kwargs)
        t.start()
    return inner
