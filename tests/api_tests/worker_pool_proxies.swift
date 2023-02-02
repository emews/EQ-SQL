/**
   EMEWS loop.swift
*/

import assert;
import io;
import python;
import string;
import sys;

import EQ;
import emews;

int SIM_WORK_TYPE = 1;
string WORKER_POOL_ID = "proxy_pool";

// string tmp_dir = "%s/tmp" % getenv("TURBINE_OUTPUT");

string task_code = """
from eqsql import proxies
import json


f_str = r'%s'
f = proxies.load_proxies({'f': json.loads(f_str)})['f']
proxy_str = r'%s'
params_str = r'%s'
r = f(proxy_str, params_str)
""";

string parse_params_code = """
import json
# r (raw string) is required here 
payload = json.loads(r'%s')
# print(payload, flush=True)
result = '{}|{}|{}'.format(json.dumps(payload['func']), json.dumps(payload['proxies']), json.dumps(payload['parameters']))
""";


(string result)result_to_json(string vals) {
  result = python_persist(
----
import json
l = [%s]
result = json.dumps(l)
---- % vals, "result");
}

// app (file out, file err) app_run_eval(string func, string proxies, string params) {
//   "bash" eval_sh func proxies params @stdout=out @stderr=err;
// }

// (string result)run_eval(string func, string proxies, string params, string tmp_dir, int idx) {
//   string out_fname = "%s/out_%d.txt" % (tmp_dir, idx);
//   string err_fname = "%s/err_%d.txt" % (tmp_dir, idx);
//   // printf(out_fname);
//   file out <out_fname>;
//   file err <err_fname>;
//   (out, err) = app_run_eval(func, proxies, params) =>
//   result = "OK";
// }

(string result)run_eval(string func, string proxies, string params) {
  tc = task_code % (func, proxies, params);
  result = python_persist(tc, "str(r)");
}

(void v)
loop()
{
  for (boolean b = true;
       b;
       b=c)
  {
    message msg = eq_task_query(SIM_WORK_TYPE, WORKER_POOL_ID);
    boolean c;
    if (msg.msg_type == "status") {
      if (msg.payload == "EQ_STOP") {
        printf("loop.swift: FINAL") =>
          v = propagate() =>
          c = false;
        // finals = EQ_get();
        // printf("Swift: finals: %s", finals);
      } else {
        printf("loop.swift: got %s: exiting!", msg.payload) =>
        v = propagate() =>
        c = false;
      }
    } else {
      int eq_task_id = msg.eq_task_id;
      // payload consists of proxies and parameters
      string params_code = parse_params_code % msg.payload;
      string payload_parts[] = split(python_persist(params_code, "result"), "|");
      string params[] = parse_json_list(payload_parts[2]);
      string results[];
      foreach p,i in params
      {
        // string code = task_code % (payload_parts[0], payload_parts[1], p);
        // results[i] = python_persist(code, "r");
        results[i] = run_eval(payload_parts[0], payload_parts[1], p);
      }
      result = join(results, ",");
      json_result = result_to_json(result);
      eq_task_report(eq_task_id, SIM_WORK_TYPE, json_result) => c = true;
    }
  }

}

loop() => printf("loop.swift: normal exit.");
