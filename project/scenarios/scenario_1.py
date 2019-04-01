# import time
# import random
from project.main import deposit, withdraw
from project.utils import threadedtask


@threadedtask
def operation1():
    deposit(username='john', pin=1234, accountid=1, amount=20000, req_id='1_D')
    withdraw(username='john', pin=1234, accountid=1, amount=500, req_id='1_W')


@threadedtask
def operation2():
    withdraw(username='john', pin=1234, accountid=1, amount=500, req_id='2_W')


def run():
    print('\nRunning scenario 1')
    operation1()
    operation2()
