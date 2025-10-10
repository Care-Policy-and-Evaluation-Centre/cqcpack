# custom_lib <- "C:/Users/DASR6/Desktop/cqc.rpack/Rlibs"
# dir.create(custom_lib, recursive = TRUE, showWarnings = FALSE)
# .libPaths(c(custom_lib, .libPaths()))
# .libPaths()

# packages <- c("devtools", "usethis", "roxygen2", "data.table", 
#               "jsonlite", "httr", "purrr", "stringr", "tibble", "dplyr")
# install.packages(packages, lib = custom_lib)


# Reload package
# library(devtools)
# pkg_path <- "C:/Users/DASR6/Desktop/cqc.rpack"
# devtools::load_all(pkg_path)
# devtools::check(pkg_path)
# devtools::build(pkg_path)
# devtools::install(pkg_path)
# install.packages("C:/Users/DASR6/Desktop/cqc.rpack")
# library(cqcpack)

if(interactive() || Sys.getenv("GITHUB_ACTIONS") == "true") { # Dev-only file, not for R CMD CHECK
  devtools::load_all()
  devtools::check()
  devtools::build()
  devtools::install()
  library(cqcpack)

#-------------------------------------------------------------------------------
# CACHING AND BUILDING DF
#-------------------------------------------------------------------------------
# LOCATION DF
# 1. Caching location IDs to project file
location_ids <- cache_location_ids()

# 2. Caching location JSONs to project file
location_jsons <- cache_location_jsons()

# 3. Building location_df from most recent scrape
location_df <- build_location_df(update_mode = FALSE)
View(location_df)

#-------------------------------------------------------------------------------
# CACHING AND BUILDING DF
#-------------------------------------------------------------------------------
# PROVIDER DF
# 1. Caching provider IDs to project file
provider_ids <- cache_provider_ids()

# 2. Caching provider JSONs to project file
provider_jsons <- cache_provider_jsons()

# 3. Building provider_df from most recent scrape
provider_df <- build_provider_df()
View(provider_df)

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
View(merged_data)

#-------------------------------------------------------------------------------
# GETTING ID SPECIFIC INFORMATION (last tested on 16.04.2025), (updated on 28.07.2025)
#-------------------------------------------------------------------------------
# Getting provider id information
provider_results <- get_cqc_id_info(c("1-101604132"), id_type = "provider")
provider_results

# Getting location id information
location_results <- get_cqc_id_info(c("1-227235701"), id_type = "location")
location_results

devtools::build()

}