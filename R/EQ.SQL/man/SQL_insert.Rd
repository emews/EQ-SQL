% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/eq.R
\name{SQL_insert}
\alias{SQL_insert}
\title{Utility function for inserting the specified values into the specified
columns in the specified table.}
\usage{
SQL_insert(table, col_names, values)
}
\arguments{
\item{table}{String. The name of the table to insert to}

\item{col_names}{List. A list of column names to insert to}

\item{values}{List. A list of values to insert into the columns}
}
\value{
the result of the insert as a DBI result.
}
\description{
Utility function for inserting the specified values into the specified
columns in the specified table.
}
