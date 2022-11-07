from mpi4py import MPI
import asyncio
import argparse

from eqsql import eq


# IMPORTANT ENV VARIABLE:
# * EQ_DB_RETRY_THRESHOLD sets the db connection retry threshold for querying and reporting
# * EQ_QUERY_TASK_TIMEOUT sets the query task timeout.

TASK_RESULT = 0
DONE = 1

RESULT = 0
REQUEST = 1


def do_work(rank, payload):
    pass


async def get_tasks(work_type, q: asyncio.Queue):
    while True:
        msg_map = eq.query_task(work_type, timeout=0)
        task_type = msg_map['type']
        payload = msg_map['payload']
        if task_type == 'work':
            # print(f'Task: {msg_map}', flush=True)
            await q.put(msg_map)
        elif payload == eq.EQ_STOP:
            # print(f'Task: {msg_map}', flush=True)
            await q.put(msg_map)
            break
        elif payload == eq.EQ_ABORT:
            # TODO handle this better
            break
        await asyncio.sleep(0)
    print('Get Tasks Done', flush=True)


async def distribute_tasks(comm, q: asyncio.Queue):
    live_ranks = comm.Get_size() - 1
    status = MPI.Status()
    stop_task = None
    while live_ranks > 0:
        has_msg = comm.iprobe(source=MPI.ANY_SOURCE, tag=REQUEST)
        if has_msg:
            comm.recv(source=MPI.ANY_SOURCE, tag=REQUEST, status=status)
            source = status.Get_source()
            if stop_task is None:
                task = await q.get()
                comm.send(task, dest=source, tag=REQUEST)
                if task['type'] == 'status':
                    # EQ_STOP
                    stop_task = task
                    live_ranks -= 1
            else:
                comm.send(stop_task, dest=source, tag=REQUEST)
                live_ranks -= 1

        await asyncio.sleep(0)
    print('Distribute Tasks Done', flush=True)


async def get_results(comm, work_type: int):
    live_ranks = comm.Get_size() - 1
    while live_ranks > 0:
        has_request = comm.iprobe(source=MPI.ANY_SOURCE, tag=RESULT)
        if has_request:
            result = comm.recv(source=MPI.ANY_SOURCE, tag=RESULT)
            if result['type'] == DONE:
                live_ranks -= 1
            else:
                eq_task_id = result['eq_task_id']
                payload = result['payload']
                eq.report_task(eq_task_id, work_type, payload)

        await asyncio.sleep(0)

    print('Get Results Done')


def run_worker(comm):
    alive = True
    rank = comm.Get_rank()
    while alive:
        # print(f'Rank {rank} requesting work', flush=True)
        comm.send(None, dest=0, tag=REQUEST)
        task = comm.recv(source=0)
        task_type = task['type']
        payload = task['payload']
        # print(f'Rank: {rank} received work {task}', flush=True)
        if task_type == 'work':
            json_result = do_work(rank, payload)
            eq_task_id = task['eq_task_id']
            msg = {'type': TASK_RESULT, 'eq_task_id': eq_task_id, 'payload': json_result}
            # print(f'Rank {rank} sending {msg}', flush=True)
            comm.send(msg, dest=0, tag=RESULT)
        elif payload == eq.EQ_STOP:
            alive = False
            msg = {'type': DONE}
            comm.send(msg, dest=0, tag=RESULT)

    print(f'Rank {comm.Get_rank()} Done', flush=True)


async def run_server(comm, work_type):
    work_queue = asyncio.Queue()
    # qt = asyncio.create_task(get_tasks(work_type, work_queue))
    # dt = asyncio.create_task(distribute_tasks(comm, work_queue))
    # grt = asyncio.create_task(get_results(comm, work_type))

    await asyncio.gather(get_tasks(work_type, work_queue),
                         distribute_tasks(comm, work_queue),
                         get_results(comm, work_type))


def run(work_type: int):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0:
        eq.init()
        try:
            asyncio.run(run_server(comm, work_type))
        finally:
            eq.close()
    else:
        run_worker(comm)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Show stats for EXPID")
    parser.add_argument('work_type', type=int)
    args = parser.parse_args()
    run(args['work_type'])
