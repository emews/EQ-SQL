clear_db_sql = """
delete from eq_exp_id_tasks;
delete from eq_tasks;
delete from emews_queue_OUT;
delete from emews_queue_IN;
delete from eq_task_tags;
alter sequence emews_id_generator restart;
"""


def clear_db(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(clear_db_sql)
