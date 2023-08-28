library(reticulate)

#' Initializes the Python "eqsql" package on which this R package
#' depends. The object returned by this function call can be used
#' to call the various python functions in that package. See the reticulate
#' R package documentation for more information about
#' calling Python from R: https://rstudio.github.io/reticulate/index.html.
#' 
#' @param python_path The path to the Python executable to use
#' to import and run the Python code used by this package. Defaults
#' to NULL in which case the first Python found will be used.
#' @param eqsql_path The path to the Python eqsql package. Defaults to
#' NULL in which case the package is assumed to be installed with
#' the specified Python.
#' @return a reticulate Python package object.
#' @examples
#' eqsql <- init_eqsql()
#' # get the ResultStatusSuccess value from the Python eq module
#' # in the eqsql package
#' success <- eqsql$eq$ResultStatus$SUCCESS
#' @export
init_eqsql <- function(python_path = NULL, eqsql_path = NULL) {
    if (!is.null(python_path)) {
        use_python(python_path, required = T)
    }
    
    if (is.null(eqsql_path)) {
        eqsql <- import('eqsql')
        import('eqsql.eq')
        import('eqsql.db_tools')
    } else {
        eqsql <- import_from_path('eqsql', path = eqsql_path)
        import_from_path('eqsql.eq', path = eqsql_path)
        import_from_path('eqsql.db_tools', path = eqsql_path)
    }

    eqsql
}

#' Initializes and returns a reticulate wrapped Python task_queue
#' (an eqsql.eq.EQSQL class instance) with the specified parameters.
#' See the reticulate R package documentation for more information about
#' calling Python from R: https://rstudio.github.io/reticulate/index.html.
#' 
#' @param db_port The port of the database to connect to. 
#' @param db_name The name of the database to connect to. 
#' @param db_host The host of the database to connect to. 
#' @param db_user The user name to connect to the database with. 
#' @param log_level the logging threshold level. Defaults to logger::WARN.
#' @return a reticulate wrapped Python instance of an eqsql.eq.EQSQL class
#' that can be used as a task queue. 
#' @export
#'
init_task_queue <- function(eqsql, db_host, db_user, db_port, db_name, retry_threshold = 0, 
                       log_level=logger::WARN) {
    if (log_level == logger::TRACE || log_level == logger::DEBUG) {
        pylog <- 10
    } else if (log_level == logger::WARN) {
        pylog <- 30
    } else if (log_level == logger::INFO) {
        pylog <- 20
    } else if (log_level == logger::ERROR) {
        pylog <- 40
    } else if (log_level == logger::FATAL) {
        pylog <- 50
    } else {
        pylog <- 20
    }

    task_queue <- eqsql$eq$init_task_queue(db_host, db_user, db_port, db_name, retry_threshold, log_level)
}

#' Applies the specified function to the specified list of futures as they
#' complete.  he futures are checked for completion by iterating over all of the
#' ones that have not yet completed and checking for a result. At the end of 
#' each iteration, the timeout is checked. A TimeoutError will be raised if the 
#' futures do not complete within the specified timeout duration.
#' @param eqsql an eqsql Python package object as returned from init_eqsql.
#' @param futures the list of Python eqsql.eq.Future objects to apply the
#' function to
#' @param func the function to be applied
#' @param ... optional arguments to func
#' @param pop if true, completed futures will be popped off of the returned
#' futures argument list
#' @param timeout if the time taken for futures to completed is greater than 
#' this value, then raise TimeoutError.
#' @param sleep the time, in seconds, to sleep between each iteration over all the Futures.
#' @return a list with two elements. fts: the futures argument list, with
#' the completed futures omitted if the pop argument is true, and f_results:
#' a list containing the result of the function application.
#' @export
as_completed <- function(eqsql, futures, func, ..., pop = FALSE, n = NULL, timeout = NULL,
                         sleep = 0.0) {
  iter <- eqsql$eq$as_completed(futures, pop = pop, n = n, timeout = timeout, sleep = sleep)
  args <- list(...)
  completed_ids <- c()
  results <- c()
  while (T) {
    ft <- iter_next(iter, completed = NULL)
    if (is.null(ft)) break
    r <- do.call(func, c(ft, args))
    results <- append(results, r)
    completed_ids <- append(completed_ids, ft$eq_task_id)
  }
  
  if (pop) {
    fts <- discard(futures, function(ft) ft$eq_task_id %in% completed_ids)
    return(list(fts=fts, f_results=results))
  }
  return(list(fts=futures, f_results=results))
}

#' Pops and returns the first completed future from the specified List
#' of Futures.
#' @param eqsql an eqsql Python package object as returned from init_eqsql.
#' @param futures the list of Python eqsql.eq.Future objects from which
#' to get the first completed.
#' @param timeout if the time taken for futures to completed is greater than 
#' this value, then raise TimeoutError.
#' @param sleep the time, in seconds, to sleep between each iteration over all the Futures.
#' @return a list with two elements. fts: the futures argument list with
#' the completed future omitted, and ft: the completed future.
pop_completed <- function(eqsql, futures, timeout = NULL, sleep = 0.0) {
  iter <- eqsql$eq$as_completed(futures, pop = T, n = 1, timeout = timeout, 
                                sleep = sleep)
  ft <- iter_next(iter)
  fts <- discard(futures, function(x) x$eq_task_id == ft$eq_task_id)
  return(list(fts=fts, ft=ft))
}

# _as_completed <- generator(function(eqr, futures, pop = FALSE, n = -1) {
#   start_time <- Sys.time()
#   
#   completed_ids <- c()
#   wk_fts <- futures
#   n_futures <- length(wk_fts)
#   while (T) {
#     for (ft in wk_fts) {
#       if (! ft$eq_task_id %in% completed_ids) {
#         result <- ft$result(timeout=0.0)
#         status <- result[[1]]
#         result_str <- result[[2]]
#         if (status == eqr$eq$ResultStatus$SUCCESS || result_str == er$EQ_ABORT) {
#           completed_ids <- append(completed_ids, ft$eq_task_id)
#           if (pop) {
#             # python object equality doesn't seem to translate through
#             # reticulate so need to check for task_id match
#             idx <- detect_index(futures, function(x) x$eq_task_id == ft$eq_task_id)
#             futures[[idx]] <<- NULL
#           }
#           yield(ft)
#           n_completed <- length(completed_ids)
#           if (n_completed == n || n_completed == n_futures) return(exhausted())
#         }
#       }
#     }
#     # ft_tasks <- discard(ft_tasks, function(ft) ft$eq_task_id %in% completed_ids)
#   }
# })
