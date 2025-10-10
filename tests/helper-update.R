
if(interactive() || Sys.getenv("GITHUB_ACTIONS") == "true") { # Dev-only file, not for R CMD CHECK


library(cqcpack)
#-------------------------------------------------------------------------------
# UPDATING CHANGES
#-------------------------------------------------------------------------------
# 1. Getting incremental location changes
changes <- get_incremental_changes()

# LOCATION DF
# 2.1 Update location df with changes between last scrape and today
update_location_dataset()

# PROVIDER DF
# 2.2 Update Location df with changes between last scrape and today
update_provider_dataset()
#-------------------------------------------------------------------------------
# MERGING DF (its working now , 11.08.2025)
#-------------------------------------------------------------------------------
merged_data <- merge_provider_location()

  
# Record build date (ISO) in DESCRIPTION
if (!requireNamespace("desc", quietly = TRUE)) {
  stop("Please install the 'desc' package to write build metadata")
}
d <- desc::desc(file = "DESCRIPTION")

# Update date built  
d$set("DataBuilt", format(Sys.Date()))   # e.g. "2025-09-03"

# Update version
version <- d$get("Version")  |>
  strsplit(".", fixed = TRUE)  |>
    unname()  |>
      unlist()  |>
        as.integer()
minor_num_idx <- length(version)
version[minor_num_idx] <- version[minor_num_idx] + 1
new_version <- version  |>
as.character()  |>
  paste0(collapse = ".")
d$set("Version", new_version)
d$write()
  
# move old versions to archive
old_versions <- list.files("../", pattern = "^cqcpack_.+\\.tar\\.gz", full.names = TRUE)

for(old_version in old_versions) {
  file.copy(old_version, sub("../", "../archive/", old_version, fixed = TRUE))
  unlink(old_version)
}

devtools::build()
}

