from multiprocessing import Pool
import time
from eqsql import eq
import random


def connect(i):
    host = 'localhost'
    user = 'eqsql_user'
    port = 5433
    db_name = 'eqsql_db'

    eq.init_task_queue(host, user, port, db_name, retry_threshold=100)
    time.sleep(random.randint(2, 5))
    eq.close()


def run():
    with Pool(500) as p:
        p.map(connect, [x for x in range(1000)])


if __name__ == '__main__':
    run()
