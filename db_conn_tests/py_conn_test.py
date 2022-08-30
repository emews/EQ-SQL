import os
from multiprocessing import Pool
import time
import eq
import random


def connect(i):
    eq.init(retry_threshold=100)
    time.sleep(random.randint(2, 5))
    eq.close()


def setup():
    os.environ['DB_HOST'] = 'localhost'
    os.environ['DB_USER'] = 'eqsql_user'
    os.environ['DB_PORT'] = '5433'
    os.environ['DB_NAME'] = 'eqsql_db'


def run():
    setup()
    with Pool(500) as p:
        p.map(connect, [x for x in range(1000)])


if __name__ == '__main__':
    run()
