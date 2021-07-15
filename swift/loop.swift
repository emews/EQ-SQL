
/**
   EMEWS workflow.swift
*/

import assert;
import io;
import python;
import string;
import sys;

import EQ;

/** The objective function */
(string result)
task(string params)
{
  result = python_persist(
----
import sys
from math import sin,cos
x,y=%s
result = sin(4*x)+sin(4*y)+-2*x+x**2-2*y+y**2
print("TASK: " + str(x) + " " + str(y) + " -> " + str(result))
sys.stdout.flush()
---- % params,
"repr(result)"
  );
}

(void v)
loop()
{
  for (boolean b = true;
       b;
       b=c)
  {
    message = EQ_get();
    // printf("swift: message: %s", message);
    boolean c;
    if (message == "FINAL")
    {
      printf("loop.swift: FINAL") =>
        v = propagate() =>
        c = false;
      // finals = EQ_get();
      // printf("Swift: finals: %s", finals);
    }
    else if (message == "EQ_ABORT")
    {
      printf("loop.swift: got EQ_ABORT: exiting!") =>
        v = propagate() =>
        c = false;
    }
    else
    {
      string params[] = split(message, ";");
      string results[];
      foreach p,i in params
      {
        results[i] = task(p);
      }
      result = join(results, ";");
      // printf("swift: result: %s", result);
      EQ_put(result) => c = true;
    }
  }

}

loop();
