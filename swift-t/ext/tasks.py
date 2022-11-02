import logging
import traceback

from eqsql import eq


def query_task(eq_work_type: int, query_timeout: float = 120.0,
               retry_threshold: int = 0, log_level=logging.WARN):
    eq.init(retry_threshold, log_level)
    try:
        eq.logger.debug('swift out_get')
        # result is a msg map
        msg_map = eq.query_task(eq_work_type, timeout=query_timeout)
        items = [msg_map['type'], msg_map['payload']]
        if msg_map['type'] == 'work':
            items.append(str(msg_map['eq_task_id']))
        # result_str should be returned via swift's python persist
        eq.logger.debug('swift out_get done')
        return '|'.join(items)
    except Exception:
        eq.logger.error(f'tasks.query_task error {traceback.format_exc()}')
        # result_str returned via swift's python persist
        return eq.ABORT_JSON_MSG
    finally:
        eq.close()


def report_task(eq_task_id: int, eq_work_type: int, result_payload: str,
                retry_threshold: int = 0, log_level=logging.WARN):
    eq.init(retry_threshold, log_level)
    try:
        # TODO this returns a ResultStatus, add FAILURE handling
        eq.report_task(eq_task_id, eq_work_type, result_payload)
    except Exception:
        eq.logger.error(f'tasks.report_task error {traceback.format_exc()}')
    finally:
        eq.close()
