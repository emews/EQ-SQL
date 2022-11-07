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


string emews_root = getenv("EMEWS_PROJECT_ROOT");
string turbine_output = getenv("TURBINE_OUTPUT");

int SIM_WORK_TYPE = string2int(argv("sim_work_type", 1));

// IMPORTANT ENV VARIABLE:
// * EQ_DB_RETRY_THRESHOLD sets the db connection retry threshold for querying and reporting
// * EQ_QUERY_TASK_TIMEOUT sets the query task timeout.

// Example code for using a proxied function 'f'
string task_code = """
from eqsql import proxies
import json


f_str = r'%s'
f = proxies.load_proxies({'f': json.loads(f_str)})['f']
proxy_str = r'%s'
params_str = r'%s'
r = f(proxy_str, params_str)
""";


(string result)run(string func, string proxies, string params) {
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
    message msg = eq_task_query(SIM_WORK_TYPE);
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
        results[i] = run(payload_parts[0], payload_parts[1], p);
      }
      result = join(results, ",");
      json_result = result_to_json(result);
      eq_task_report(eq_task_id, SIM_WORK_TYPE, json_result) => c = true;
    }
  }

}

loop() => printf("loop.swift: normal exit.");
