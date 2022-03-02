
/**
   EMEWS loop.swift
*/

import assert;
import io;
import python;
import string;
import sys;
import emews;

import EQ;

/** The objective function */
(string result)
task(string params)
{
  result = python_persist(
----
import sys
import json
from math import sin,cos

val_map = json.loads('%s')
x = val_map['x']
y = val_map['y']
result = sin(4*x)+sin(4*y)+-2*x+x**2-2*y+y**2
print("TASK: " + str(x) + " " + str(y) + " -> " + str(result), flush=True)
---- % params, "repr(result)");
}

(string result)result_to_json(string vals) {
  result = python_persist(
----
import json
l = [%s]
result = json.dumps(l)
---- % vals, "result");
}

int SIM_WORK_TYPE = 1;

/*
(string result)
task_json(string params)
{
  result = python_persist(
----
import json, sys
from math import sin, cos
D = json.loads(params)
values = D["values"]
x, y = values[0], values[1]
result = sin(4*x)+sin(4*y)+-2*x+x**2-2*y+y**2
print("TASK: " + str(x) + " " + str(y) + " -> " + str(result))
sys.stdout.flush()
---- % params,
"repr(result)"
  );
}
*/

(void v)
loop()
{
  for (boolean b = true;
       b;
       b=c)
  {
    message msg = eq_task_querier(SIM_WORK_TYPE);
    boolean c;
    if (msg.msg_type == "status") {
      if (msg.payload == "EQ_FINAL") {
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
      string params[] = parse_json_list(msg.payload);
      string results[];
      foreach p,i in params
      {
        results[i] = task(p);
      }
      result = join(results, ",");
      printf("RESULT: %s", result);
      json_result = result_to_json(result);
      // printf("JSON RESULT: %s", json_result);
      eq_task_reporter(eq_task_id, SIM_WORK_TYPE, json_result) => c = true;
    }
  }

}

loop() => printf("loop.swift: normal exit.");
