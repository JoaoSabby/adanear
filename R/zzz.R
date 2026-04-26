#' @noRd
.onLoad <- function(libname, pkgname) {
  if (!requireNamespace("dials", quietly = TRUE)) {
    return(invisible())
  }

  dials_ns <- asNamespace("dials")

  if (!exists("tunable", envir = dials_ns, inherits = FALSE)) {
    return(invisible())
  }

  utils::registerS3method(
    generic = "tunable",
    class = "step_adanear",
    method = tunable.step_adanear,
    envir = dials_ns
  )

  invisible()
}
