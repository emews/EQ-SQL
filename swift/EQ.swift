
/*
   EMEWS EQ.swift for SQL
*/

(string result) EQ_validate() {
    result = python_persist("import eq ; eq.init()", "eq.validate()");
}

(void v) EQ_init_package(location loc, string packageName) {
    v = propagate();
}

EQ_stop(location loc) {
    // do nothing
}

string get_string = "result = eq.OUT_get()";

(string result) EQ_get(location loc){
    string code = get_string;
    result = python_persist(code, "result");
}

string put_string = "eqpy.IN_put('%s')";

(void v) EQPy_put(location loc, string data){
    string code = put_string % data;
    python_persist(code) => v = propagate();
}

// Local Variables:
// c-basic-offset: 4
// End:
