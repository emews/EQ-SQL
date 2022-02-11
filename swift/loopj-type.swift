
/**
   EMEWS loopj-type.swift : uses JSON
*/

import assert;
import io;
import python;
import string;
import sys;

import EQ;

int eq_type = string2int(argv("eq-type", "0"));

/**
   The objective function
   params: A JSON string
   See taskj.py for the JSON structure
 */
(string result)
task(string eq_id)
{
  result = python_persist(
----
from taskj import f
eq_id = %i
result = f(eq_id)
---- % eq_id,
"str(result)"
  );
}

(void v)
loop()
{
  for (boolean b = true;
       b;
       b=c)
  {
    message = EQ_get(eq_type);
    // printf("swift: message: %s", message);
    boolean c;
    if (message == "EQ_FINAL")
    {
      printf("loopj-type.swift: FINAL") =>
        v = propagate() =>
        c = false;
      // finals = EQ_get();
      // printf("Swift: finals: %s", finals);
    }
    else if (message == "EQ_ABORT")
    {
      printf("loopj-type.swift: got EQ_ABORT: exiting!") =>
        v = propagate() =>
        c = false;
    }
    else
    {
      string eq_ids[] = split(message, ";");
      string results[];
      foreach eq_id, i in eq_ids
      {
        results[i] = task(eq_id);
      }
      result = join(results, ";");
      // printf("swift: result: %s", result);
      EQ_put(eq_type, result) => c = true;
    }
  }
}

loop() => printf("loopj-type.swift: loop exit.");
