% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/eq.R
\name{update_task}
\alias{update_task}
\title{Updates the specified task in the eq_tasks table with the specified
result ("json_in") payload}
\usage{
update_task(eq_task_id, payload)
}
\arguments{
\item{eq_task_id}{Integer. The id of the task to update}

\item{payload}{String. The payload to update the task with}
}
\value{
ResultStatus$SUCCESS if the task was successfully updated,
otherwise ResultStatus$FAILURE.
}
\description{
This also updates the "time_stop" to the time when the update occurred.
}
