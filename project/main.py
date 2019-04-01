import json
import os
from contextlib import contextmanager
from functools import wraps

from project import (LEDGER_TYPE_CREDIT, LEDGER_TYPE_DEBIT,
                     authenticate_and_verify_account, get_balance,
                     ledger_entry, update_balance)
from project.utils import on_deposit_failure, on_withdraw_failure
from psycopg2.pool import ThreadedConnectionPool

DIR = os.path.dirname(__file__)
DB_CONFIG = os.path.join(DIR, './config/db-connection.json')

SCHEMA_PATH = os.path.join(DIR, './sql/schema.sql')
FEED_PATH = os.path.join(DIR, './sql/dogfeed.sql')


def get_db_connection_string(configpath):
    """
    Reads the database config and returns DB connection string
    """
    with open(configpath, 'r') as f:
        config = json.load(f)
        DB_NAME = config['db']
        USER = config['user']
        PASSWORD = config['password']
        return f'dbname={DB_NAME} user={USER} password={PASSWORD}'


def connect_to_db(connections=2):
    """
    Connect to PostgreSQL and return connection
    """
    minconn = connections
    maxconn = connections * 2
    return ThreadedConnectionPool(minconn, maxconn, DB_CONN_STRING)


DB_CONN_STRING = get_db_connection_string(DB_CONFIG)
pool = connect_to_db()


@contextmanager
def transaction(*args, **kwargs):
    options = {
        'isolation_level': kwargs.get('isolation_level', None),
        'readonly': kwargs.get('readonly', None),
        'deferrable': kwargs.get('deferrable', None),
    }
    try:
        conn = pool.getconn()
        conn.set_session(**options)
        yield conn
        conn.commit()
    except Exception as e:
        print('\nROLLBACK TRANSACTION...', e)
        conn.rollback()
        failure_handler = kwargs.get('failure_handler', None)
        failure_handler and failure_handler(**kwargs)
    finally:
        """
        The method rolls back an eventual pending transaction
        and executes the PostgreSQL RESET and SET SESSION AUTHORIZATION
        to revert the session to the default values
        """
        conn.reset()
        pool.putconn(conn)


def transact(options={}):
    def transact_inner(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            options.update(kwargs)
            with transaction(fn.__name__, *args, **options) as conn:
                fn(conn, *args, **kwargs)
        return wrapper
    return transact_inner


@transact()
def bulk_operation(conn, path, *args, **kwargs):
    """
    DROP/ CREATE schema from file
    """
    with open(path, 'r') as f:
        sql = f.read()
    with conn.cursor() as curs:
        curs.execute(sql)


@transact({
    'isolation_level': 'SERIALIZABLE',
    'failure_handler': on_deposit_failure,
})
def deposit(conn, req_id, username, pin, accountid, amount):
    userid = authenticate_and_verify_account(
        conn, username, pin, accountid, req_id)
    ledger_entry(conn, accountid, LEDGER_TYPE_CREDIT, amount, req_id)
    update_balance(conn, userid, accountid, amount, req_id)
    balance = get_balance(conn, userid, accountid, req_id)

    print(f'\n{req_id} > Deposit: {amount}, Balance: {balance} <{username}>')


@transact({
    'isolation_level': 'SERIALIZABLE',
    'failure_handler': on_withdraw_failure,
})
def withdraw(conn, req_id, username, pin, accountid, amount):
    userid = authenticate_and_verify_account(
        conn, username, pin, accountid, req_id)
    ledger_entry(conn, accountid, LEDGER_TYPE_DEBIT, amount, req_id)
    update_balance(conn, userid, accountid, amount * -1, req_id)
    balance = get_balance(conn, userid, accountid, req_id)

    print(f'\n{req_id} > Withdraw: {amount}, Balance: {balance} <{username}>')


def main(options):
    """
    Operations on a connection is performed by the cursors from connection.
    Every connections starts a new transaction unless committed or rollback.
    So no matter the first cursor performing a execute or subsequent cursor
    All the executions will not be persisted to Database unless committed.

    By default even a simple SELECT will start a transaction.

    In long-running programs, if no further action is taken,
    the session will remain “idle in transaction”. This is an
    undesirable condition for several reasons
    (locks are held by the session, tables bloat…).

    For long lived scripts, either make sure to terminate a transaction
    as soon as possible or use an autocommit connection
    """
    if len(options) > 0 and options[0] == '--flush':
        bulk_operation(SCHEMA_PATH)
        bulk_operation(FEED_PATH)

    from scenarios import scenario_1
    scenario_1.run()


if __name__ == '__main__':
    options = []
    if len(os.sys.argv) > 1:
        options = os.sys.argv[1: len(os.sys.argv)]
        if options[0] == '--help':
            print("""Usage:
                python main.py --help
                python main.py
                python main.py --flush
            """)
            exit()

    main(options)
