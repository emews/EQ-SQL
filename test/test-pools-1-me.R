
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
  for (type in 1:types) {
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
  }
}

# eq.OUT_put(sprintf("message:%i", 42))

print("R POOLS 1 ME: STOP")

# Local Variables:
# mode: R;
# eval: (setq ess-default-style 'DEFAULT)
# eval: (setq ess-indent-level 2)
# End:
