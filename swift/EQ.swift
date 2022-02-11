
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
    print('swift out_get')
    sys.stdout.flush()
    result = eq.OUT_get(%i)
    print('swift out_get done')
    sys.stdout.flush()
except Exception as e:
    import sys, traceback
    info = sys.exc_info()
    s = traceback.format_tb(info[2])
    print(str(e) + ' ... \\n' + ''.join(s))
    sys.stdout.flush()
    result = 'EQ_ABORT'
""";

(string result) EQ_get(int eq_type) {
    result = python_persist(code_get % eq_type, "result");
}

string code_put = """
import sys
import eq
eq.init()
try:
    print('swift in_put')
    sys.stdout.flush()
    result = eq.IN_put(%i, '%s')
except Exception as e:
    import sys, traceback
    info = sys.exc_info()
    s = traceback.format_tb(info[2])
    print(str(e) + ' ... \\n' + ''.join(s))
    sys.stdout.flush()
    result = 'EQ_ABORT'
""";

(void v) EQ_put(int eq_type, string eq_ids) {
    // trace("code: " + code_put % (eq_type, eq_ids));
    python_persist(code_put % (eq_type, eq_ids)) =>
        v = propagate();

}

// Local Variables:
// c-basic-offset: 4
// End:
