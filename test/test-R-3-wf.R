suppressMessages(library(EQ.SQL))
library(jsonlite)

print("R TEST 3 WF: START")

if (! eq_init()) {
  quit(status=1)
}

while (TRUE) {
    msg_map <- eq_query_task(0)
    # print(sprintf("Message Map: %s", msg_map))
    payload <- msg_map$payload
    if (msg_map$type != "work") {
        break
    }
    params <- fromJSON(payload)
    # create numeric vector from the param values in the payload
    result <- lapply(params$param, function(x) x)
    # print(toJSON(result, auto_unbox=T))
    eq_report_task(msg_map$eq_task_id, 0, toJSON(result, auto_unbox = T))
}

print("R TEST 3 WF: STOP")