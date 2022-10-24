suppressMessages(library(EQ.SQL))
library(jsonlite)

print("R TEST 2 WF: START")

if (! eq_init()) {
  quit(status=1)
}

while (TRUE) {
    msg_map <- eq_query_task(0)
    # print(sprintf("Message Map: %s", msg_map))
    if (msg_map$type != "work") {
        break
    }
    payload <- msg_map$payload
    params <- fromJSON(payload)
    result <- toJSON(list(result = params$p), auto_unbox = T)
    eq_report_task(msg_map$eq_task_id, 0, result)
}

print("R TEST 2 WF: STOP")