# Helper to read build date from DESCRIPTION and return age in days
.data_age_days <- function(pkg = utils::packageName()) {
  built <- utils::packageDescription(pkg, fields = "DataBuilt")
  if (is.na(built) || is.null(built) || !nzchar(built)) return(NA_integer_)
  bd <- try(as.Date(built), silent = TRUE)
  if (inherits(bd, "try-error") || is.na(bd)) return(NA_integer_)
  list(age = as.integer(Sys.Date() - bd), built_date = built)
}


.onAttach <- function(libname, pkgname) {
  if (!isTRUE(getOption("cqc.startup_check", TRUE))) return(invisible())

  age_list <- .data_age_days(pkgname)
  age <- age_list$age
  if (is.na(age)) return(invisible())

  threshold <- getOption("cqc.data_stale_after_days", 30L)
  if (is.finite(threshold) && age >= threshold) {
    
    msg <- sprintf(
      "Data in %s was built %d days ago (DataBuilt in DESCRIPTION is %s). Consider reinstalling to refresh.",
      pkgname, age, age_list$built_date
    )
    if (isTRUE(getOption("cqc.warn_on_stale", TRUE))) {
      warning(msg, call. = FALSE)
    } else {
      packageStartupMessage(paste0("Error", msg,
        " You can adjust with options(cqc.data_stale_after_days = ...)."))
    }
  }
}