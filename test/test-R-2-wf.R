suppressMessages(library(EQ.SQL))
library(jsonlite)

print("R TEST 2 WF: START")

if (! eq.init()) {
  quit(status=1)
}

while (TRUE) {
    msg_map <- eq.query.task(0)
    # print(sprintf("Message Map: %s", msg_map))
    payload <- msg_map$payload
    if (eq.done(payload)) break
    params <- fromJSON(payload)
    result = toJSON(list(result=params$p), auto_unbox=T)
    eq.report.task(0, msg_map$eq_task_id, result)
}

print("R TEST 2 WF: STOP")