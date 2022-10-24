import io;
import sys;
import files;
import location;
import string;
import R;
import assert;
import python;
import unix;
import stats;

string emews_root = getenv("EMEWS_PROJECT_ROOT");
string turbine_output = getenv("TURBINE_OUTPUT");
string tmp_dir = "%s/tmp" % turbine_output;

file run_sh = input(emews_root+"/db_conn_tests/run_r.sh");

app (file out, file err) app_run(string rfile) {
    "bash" run_sh rfile @stdout=out @stderr=err;
}

(void v) run(string rfile, int i) {
    string out_f = "%s/%d_out.txt" % (tmp_dir, i);
    string err_f = "%s/%d_err.txt" % (tmp_dir, i);
    file out <out_f>;
    file err <err_f>;
    (out,err) = app_run(rfile) =>
    v = propagate();
}

// run(create_sh, "create_table.R", 0);
foreach i, j in [0:1050] {
    run("append_table.R", i);
}
