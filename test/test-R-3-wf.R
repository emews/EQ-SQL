suppressMessages(library(EQ.SQL))
library(jsonlite)

print("R TEST 3 WF: START")

if (! eq.init()) {
  quit(status=1)
}

while (TRUE) {
    msg_map <- eq.query.task(0)
    # print(sprintf("Message Map: %s", msg_map))
    payload <- msg_map$payload
    if (eq.done(payload)) break
    params <- fromJSON(payload)
    # create numeric vector from the param values in the payload
    result <- lapply(params$param, function(x) x)
    # print(toJSON(result, auto_unbox=T))
    eq.report.task(0, msg_map$eq_task_id, toJSON(result, auto_unbox=T))
}

print("R TEST 3 WF: STOP")