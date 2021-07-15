
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

string code_get = "import eq ; eq.init() ; result = eq.OUT_get()";

(string result) EQ_get() {
    result = python_persist(code_get, "result");
}

string code_put = "import eq ; eq.init() ; eq.IN_put('%s')";

(void v) EQ_put(string data) {
    python_persist(code_put % data) => v = propagate();
}

// Local Variables:
// c-basic-offset: 4
// End:
