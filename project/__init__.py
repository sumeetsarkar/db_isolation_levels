LEDGER_TYPE_CREDIT = 'credit'
LEDGER_TYPE_DEBIT = 'debit'


def authenticate_and_verify_account(conn, username, pin, accountid, req_id=None):
    """
    Check for matching userid/ pin in DB
    Check if userid has matching accountid
    """
    with conn.cursor() as curs:
        sql = 'SELECT 1 FROM users WHERE username=%s AND pin=%s'
        curs.execute(sql, (username, pin))
        if curs.fetchone() is None:
            raise ValueError(f'{req_id} Error: Userid/ Pin mismatch')
        sql = (
            'SELECT u.id FROM account a JOIN users u on u.id = a.user_id'
            ' WHERE u.username=%s AND a.id=%s'
        )
        curs.execute(sql, (username, accountid))
        res = curs.fetchone()
        if res is None:
            raise ValueError(
                f'{req_id}\tError: No matching account for username & account')
        return res[0]


def get_balance(conn, userid, accountid, req_id=None):
    """
    Gets account balance for given userid & accountid
    """
    with conn.cursor() as curs:
        sql = ('SELECT balance FROM account WHERE user_id=%s AND id=%s')
        curs.execute(sql, (userid, accountid))
        res = curs.fetchone()
        if res is None:
            raise ValueError(
                f'{req_id}\tError: No matching account for userid & accountid')
        return res[0]


def update_balance(conn, userid, accountid, amount, req_id=None):
    """
    Makes deposit to the account
    """
    print(f'\n{req_id} Updating account user:{userid}, id:{accountid}, amount:{amount}')
    with conn.cursor() as curs:
        # Random time sleeps to simulate delays to recreate
        # concurrent request trying to withdraw and depoist funds
        # to same account
        #
        # import random
        # import time
        # time.sleep(random.random())
        current = get_balance(conn, userid, accountid)
        # time.sleep(random.random())
        print(f'{req_id} ? Balance before update: {current}')
        sql = (
            'UPDATE account'
            ' SET balance=%s'
            ' WHERE user_id=%s AND id=%s'
        )
        res = curs.execute(sql, (current + amount, userid, accountid))
        if res is not None:
            raise ValueError(
                f'{req_id}\tError: No matching account for userid & accountid')


def ledger_entry(conn, accountid, ledgertype, amount, req_id=None):
    """
    Makes ledger entry for an amount for given userid, accountid
    & ledgertype (credit/debit)
    """
    with conn.cursor() as curs:
        sql = (
            'INSERT INTO ledger (account_id, type, amount)'
            ' VALUES (%s, %s, %s)'
        )
        res = curs.execute(sql, (accountid, ledgertype, amount))
        if res is not None:
            raise ValueError(
                f'{req_id}\tError: {res}')
