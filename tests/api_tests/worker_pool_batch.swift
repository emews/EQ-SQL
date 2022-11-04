
/**
   EMEWS loop.swift
*/

import assert;
import io;
import python;
import string;
import sys;
import unix;
import EQ;
import emews;

int resident_work_rank = string2int(getenv("RESIDENT_WORK_RANK"));

int BATCH_SIZE = 4;
int WORK_TYPE = 0;
int BATCH_THRESHOLD = 1;

string obj_code = """
import numpy as np
import json
import time
import random

xy = np.array(json.loads('%s'))
time.sleep(random.random())
sum = np.sum(xy)

""";

(string result) obj(string xy) {
  string code = obj_code % xy;
  result = python_persist(code, "str(sum)");
}

run(message msgs[]) {
  printf("MSGS SIZE: %d", size(msgs));
  foreach msg, i in msgs {
    result_payload = obj(msg.payload);
    eq_task_report(msg.eq_task_id, 0, result_payload);
  }
}


(void v) loop(location querier_loc) {
  for (boolean b = true;
       b;
       b=c)
  {
    message msgs[] = eq_batch_task_query(querier_loc);
    boolean c;
    if (msgs[0].msg_type == "status") {
      if (msgs[0].payload == "EQ_STOP") {
        printf("loop.swift: STOP") =>
          v = propagate() =>
          c = false;
      } else {
        printf("loop.swift: got %s: exiting!", msgs[0].payload) =>
        v = propagate() =>
        c = false;
      }
    } else {
      run(msgs);
      c = true;
    }
  }
}

(void o) start() {
  location querier_loc = locationFromRank(resident_work_rank);
  eq_init_batch_querier(querier_loc, BATCH_SIZE, BATCH_THRESHOLD, WORK_TYPE) =>
  loop(querier_loc) => {
    eq_stop_batch_querier(querier_loc);
    o = propagate();
  }
}

start() => printf("worker pool: normal exit.");