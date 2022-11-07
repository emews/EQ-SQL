
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

string emews_root = getenv("EMEWS_PROJECT_ROOT");
string turbine_output = getenv("TURBINE_OUTPUT");
int resident_work_rank = string2int(getenv("RESIDENT_WORK_RANK"));

int WORK_TYPE = string2int(argv("sim_work_type", 0));
int BATCH_SIZE = string2int(argv("batch_size"));
int BATCH_THRESHOLD = string2int(argv("batch_threshold", 1));

// IMPORTANT ENV VARIABLE:
// * EQ_DB_RETRY_THRESHOLD sets the db connection retry threshold for querying and reporting


(string result) run(string params) {
  // TODO
}

run(message msgs[]) {
  // printf("MSGS SIZE: %d", size(msgs));
  foreach msg, i in msgs {
    result_payload = run(msg.payload);
    eq_task_report(msg.eq_task_id, WORK_TYPE, result_payload);
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