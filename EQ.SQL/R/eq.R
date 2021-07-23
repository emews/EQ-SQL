
#' @export
eq.init <- function() {
    print("init")
}

#' @export
eq.OUT_put <- function(msg) {
    printf("OUT_put(%s)\n", msg)
}
