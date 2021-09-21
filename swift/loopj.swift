
/**
   EMEWS loopj.swift : uses JSON
*/

import assert;
import io;
import python;
import string;
import sys;

import EQ;

/**
   The objective function
   params: A JSON string
   See taskj.py for the JSON structure
 */
(string result)
task(string params)
{
  result = python_persist(
----
from taskj import f
params = '%s'
result = f(params)
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
    if (message == "EQ_FINAL")
    {
      printf("loopj.swift: FINAL") =>
        v = propagate() =>
        c = false;
      // finals = EQ_get();
      // printf("Swift: finals: %s", finals);
    }
    else if (message == "EQ_ABORT")
    {
      printf("loopj.swift: got EQ_ABORT: exiting!") =>
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

loop() => printf("loopj.swift: normal exit.");
