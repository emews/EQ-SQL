
# R TEST POOLS 1 ME

library(EQ.SQL)
library(rjson)

print("R POOLS 1 ME: START")

iterations = 3
types      = 2
samples    = 3

if (! eq.init()) {
  print("R POOLS 1 ME: EQ/SQL could not init!")
  quit(status=1)
}

for (iteration in 1:iterations) {
  printf("R POOLS 1 ME: ITERATION: %i\n", iteration)
  print ("R POOLS 1 ME: TYPES START")
  for (type in 1:types) {
    print("R POOLS 1 ME: SAMPLES START")
    for (sample in 1:samples) {
      note = sprintf("%i:%i:%i", iteration, type, sample)
      x = sample    * 0.10
      y = iteration * 0.01
      L <- list(x=x, y=y, note=note)
      D <- list(values=L)
      J = toJSON(D)
      # cat("R toJSON: ", J, "\n")
      eq.OUT_put(type, J)
    }
    for (sample in 1:samples) {
      printf("R POOLS 1 ME: GET: %i\n", type)
      eq.IN_get(type)
      printf("R POOLS 1 ME: GOT: %i\n", type)
    }
    print("R POOLS 1 ME: SAMPLES DONE")
  }  # next type
  print("R POOLS 1 ME: TYPES DONE")
}   # next iteration

# eq.OUT_put(sprintf("message:%i", 42))

print("R POOLS 1 ME: STOP")

# Local Variables:
# mode: R;
# eval: (setq ess-default-style 'DEFAULT)
# eval: (setq ess-indent-level 2)
# End:
