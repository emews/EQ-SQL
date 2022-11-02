
/**
   EMEWS workflow.swift
*/

import assert;
import io;
import location;
import python;
import string;
import sys;

import EQ_SQL;

N = 10;

/** The objective function */
(string result)
task(string params)
{
  result = python(
----
from math import sin,cos
x,y=%s
result = sin(4*x)+sin(4*y)+-2*x+x**2-2*y+y**2
print("TASK: " + str(x) + " " + str(y) + " -> " + str(result))
---- % params,
"repr(result)"
  );
}

location EQ = locationFromRank(turbine_workers()-1);

(void v)
handshake(string settings_filename)
{
  message = EQ_get(GA) =>
    v = EQ_put(GA, settings_filename);
  assert(message == "Settings", "Error in handshake.");
}

(void v)
loop(int N)
{
  for (boolean b = true;
       b;
       b=c)
  {
    message = EQ_get(EQ);
    // printf("swift: message: %s", message);
    boolean c;
    if (message == "FINAL")
    {
      printf("Swift: FINAL") =>
        v = make_void() =>
        c = false;
      finals = EQ_get(EQ);
      printf("Swift: finals: %s", finals);
    }
    else if (message == "EQ_ABORT")
    {
      printf("Swift: EQ aborted!") =>
        v = make_void() =>
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
      EQ_put(EQ, result) => c = true;
    }
  }

}

settings_filename = argv("settings");

EQ_init_package(EQ, "algorithm") =>
  handshake(settings_filename) =>
  loop(N) =>
  EQ_stop(EQ);
