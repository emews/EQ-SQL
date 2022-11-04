
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

// IMPORTANT ENV VARIABLE:
// * EQ_DB_RETRY_THRESHOLD sets the db connection retry threshold for querying and reporting
// * EQ_QUERY_TASK_TIMEOUT sets the query task timeout.

int SIM_WORK_TYPE = string2int(argv("sim_work_type", 1));

(string result) run(string params) {
  // TODO 
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
      string params[] = parse_json_list(msg.payload);
      string results[];
      foreach p,i in params
      {
        results[i] = run(p);
      }
      result = join(results, ",");
      printf("RESULT: %s", result);
      // json_result = result_to_json(result);
      // printf("JSON RESULT: %s", json_result);
      eq_task_report(eq_task_id, SIM_WORK_TYPE, json_result) => c = true;
    }
  }

}

loop() => printf("loop.swift: normal exit.");
