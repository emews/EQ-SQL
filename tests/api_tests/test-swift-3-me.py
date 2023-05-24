from eqsql import eq
import random
import json
import os


def get_new_point():
    pt = [random.random(), random.random()]
    return json.dumps(pt)


def _create_eqsql():
    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    port = int(os.getenv('DB_PORT'))
    db_name = os.getenv('DB_NAME')
    return eq.init_task_queue(host, user, port, db_name)


def run():
    eq_sql = _create_eqsql()

    try:
        init_pts = 7
        fts = []
        for _ in range(init_pts):
            payload = get_new_point()
            _, ft = eq_sql.submit_task('test-swift-3', 0, payload)
            fts.append(ft)

        num_obs = 18
        pts_obs = 0
        result_ids = []
        submitted = init_pts
        while pts_obs < num_obs:
            ft = eq.pop_completed(fts)
            pts_obs += 1

            _, result = ft.result()
            print(f'OBJ RESULT: {ft.eq_task_id} {result}', flush=True)
            result_ids.append(ft.eq_task_id)
            # add a new point
            if submitted < num_obs:
                payload = get_new_point()
                _, ft = eq_sql.submit_task('test-swift-3', 0, payload)
                fts.append(ft)
                submitted += 1
                # print("FT: ", ft.eq_task_id, flush=True)

        # do one more submission so task and stop are popped off
        # the queue together, and we can test that a list of tasks
        # including the stop works.

        payload = get_new_point()
        _, ft = eq_sql.submit_task('test-swift-3', 0, payload)
        eq_sql.stop_worker_pool(0)
        # get the result of the last submit in order to
        # clear emews_queue_in
        eq.pop_completed([ft])

    finally:
        eq_sql.close()

    print('ME DONE', flush=True)


if __name__ == '__main__':
    run()
