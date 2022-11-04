from eqsql import eq
import random
import json


def get_new_point():
    pt = [random.random(), random.random()]
    return json.dumps(pt)


def run():
    eq.init()

    try:
        init_pts = 7
        fts = []
        for _ in range(init_pts):
            payload = get_new_point()
            _, ft = eq.submit_task('test-swift-3', 0, payload)
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
                _, ft = eq.submit_task('test-swift-3', 0, payload)
                fts.append(ft)
                submitted += 1
                # print("FT: ", ft.eq_task_id, flush=True)

        # do one more submission so task and stop are popped off
        # the queue together, and we can test that a list of tasks
        # including the stop works.

        payload = get_new_point()
        _, ft = eq.submit_task('test-swift-3', 0, payload)
        eq.stop_worker_pool(0)
        # get the result of the last submit in order to
        # clear emews_queue_in
        eq.pop_completed([ft])

    finally:
        eq.close()

    print('ME DONE', flush=True)


if __name__ == '__main__':
    run()
