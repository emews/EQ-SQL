% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/eq.R
\name{eq_report_task}
\alias{eq_report_task}
\title{Reports the result of the specified task of the specified type}
\usage{
eq_report_task(eq_task_id, eq_type, result)
}
\arguments{
\item{eq_task_id}{Integer. The id of the task whose results are being
reported.}

\item{eq_type}{Integer. The type of the task whose results are being
reported.}

\item{result}{String. The result of the task.}
}
\value{
ResultStatus$SUCCESS if the task was successfully reported, otherwise
ResultStatus$FAILURE.
}
\description{
Reports the result of the specified task of the specified type
}
