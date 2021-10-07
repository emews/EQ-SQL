
/*
   EMEWS EQ.swift for SQL
*/

(string result) EQ_validate() {
    result = python_persist("import eq ; eq.init()", "eq.validate()");
}

// (void v) EQ_init_package(location loc, string packageName) {
//     v = propagate();
// }

// EQ_stop(location loc) {
//     // do nothing
// }

string code_get = """
import eq
eq.init()
try:
    result = eq.OUT_get(%i)
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

// string code_put = "import eq ; eq.init() ; eq.IN_put('%s')";

string code_put = """
import eq
eq.init()
try:
    result = eq.IN_put(%i, '%s')
except Exception as e:
    import sys, traceback
    info = sys.exc_info()
    s = traceback.format_tb(info[2])
    print(str(e) + ' ... \\n' + ''.join(s))
    sys.stdout.flush()
    result = 'EQ_ABORT'
""";

(void v) EQ_put(int eq_type, string data) {
    python_persist(code_put % (eq_type, data)) => v = propagate();
}

// Local Variables:
// c-basic-offset: 4
// End:
