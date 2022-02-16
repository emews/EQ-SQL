
/*
   EMEWS EQ.swift for SQL
*/

(string result) EQ_validate() {
    result = python_persist("import eq ; eq.init()", "eq.validate()");
}

string code_get = """
import sys
import eq
eq.init()
try:
    print('swift out_get', flush=True)
    # result is a tuple of task_id, payload
    eq_task_id, payload = eq.query_task(%i)
    result_str = '{}|{}'.format(eq_task_id, payload)
    print('swift out_get done', flush=True)
except Exception as e:
    import sys, traceback
    info = sys.exc_info()
    s = traceback.format_tb(info[2])
    print(str(e) + ' ... \\n' + ''.join(s))
    sys.stdout.flush()
    result_str = '-1|EQ_ABORT'
""";

(string result) eq_task_querier(int eq_type) {
    result = python_persist(code_get % eq_type, "result_str");
}

string code_put = """
import sys
import eq
eq.init()
try:
    print('swift in_put', flush=True)
    eq_task_id = %i
    eq_type = %i
    eq.report_task(eq_type, eq_task_id, '%s')
except Exception as e:
    import sys, traceback
    info = sys.exc_info()
    s = traceback.format_tb(info[2])
    print(str(e) + ' ... \\n' + ''.join(s))
    sys.stdout.flush()
""";

(void v) eq_task_reporter(int eq_task_id, int eq_type, string result_payload) {
    // trace("code: " + code_put % (eq_type, eq_ids));
    python_persist(code_put % (eq_task_id, eq_type, result_payload)) =>
        v = propagate();

}

// Local Variables:
// c-basic-offset: 4
// End:
