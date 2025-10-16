# custom_lib <- "C:/Users/DASR6/Desktop/cqcpack/Rlibs"
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
  
  .libPaths(c(Sys.getenv("R_LIBS"), .libPaths()))
  
  # Ensure required packages are loaded
  library(httr)
  library(jsonlite)
  library(desc)
  
  #-------------------------------------------------------------------------------
  # SETUP: Check environment and paths
  #-------------------------------------------------------------------------------
  data_repo_path <- Sys.getenv("CQC_DATA_REPO_PATH", NA_character_)
  is_github_actions <- Sys.getenv("GITHUB_ACTIONS") == "true"
  
  cat("=== CQC First-Run Build (Full Initial Sync) ===\n")
  cat("GitHub Actions:", is_github_actions, "\n")
  cat("Data repo path:", data_repo_path, "\n\n")
  
  #-------------------------------------------------------------------------------
  # BUILD AND INSTALL PACKAGE
  #-------------------------------------------------------------------------------
  cat("1. Loading cqcpack...\n")
  library(cqcpack)

  #-------------------------------------------------------------------------------
  # CACHING AND BUILDING DF
  #-------------------------------------------------------------------------------
  # LOCATION DF
  cat("2. Caching location data...\n")
  cat("   - Getting location IDs\n")
  location_ids <- cache_location_ids()
  
  cat("   - Downloading location JSONs\n")
  location_jsons <- cache_location_jsons()
  
  cat("   - Building location dataframe\n")
  location_df <- build_location_df(update_mode = FALSE)
  cat("     Total locations:", nrow(location_df), "\n")

  #-------------------------------------------------------------------------------
  # CACHING AND BUILDING DF
  #-------------------------------------------------------------------------------
  # PROVIDER DF
  # 1. Caching provider IDs to project file
  cat("3. Caching provider data...\n")
  cat("   - Getting provider IDs\n")
  provider_ids <- cache_provider_ids()
  
  cat("   - Downloading provider JSONs\n")
  provider_jsons <- cache_provider_jsons()
  
  cat("   - Building provider dataframe\n")
  provider_df <- build_provider_df()
  cat("     Total providers:", nrow(provider_df), "\n")
  
  #-------------------------------------------------------------------------------
  # MERGING DF (its working now , 11.08.2025)
  #-------------------------------------------------------------------------------
  cat("4. Merging provider and location data...\n")
  merged_data <- merge_provider_location()
  cat("   Merged dataset rows:", nrow(merged_data), "\n")

  #-------------------------------------------------------------------------------
  # COPY JSONs TO DATA REPO
  #-------------------------------------------------------------------------------
  if (is_github_actions && !is.na(data_repo_path)) {
    cat("5. Copying JSON files to data repository...\n")
    
    # Get the cache directory
    base_cache_dir <- tools::R_user_dir("cqc", "cache")
    today_date <- format(Sys.Date(), "%Y-%m-%d")
    
    # Handle location data
    loc_pattern <- paste0("location_information_", Sys.Date())
    loc_dirs <- list.dirs(base_cache_dir, full.names = TRUE, recursive = FALSE)
    loc_dirs <- loc_dirs[grepl(loc_pattern, basename(loc_dirs))]
    
    if (length(loc_dirs) > 0) {
      loc_json_dirs <- list.dirs(loc_dirs[1], full.names = TRUE, recursive = FALSE)
      loc_json_dirs <- loc_json_dirs[grepl("location_jsons_", basename(loc_json_dirs))]
      
      if (length(loc_json_dirs) > 0) {
        cat("   - Copying location JSONs...\n")
        loc_data_dir <- file.path(data_repo_path, "initial_bulk", "location")
        dir.create(loc_data_dir, recursive = TRUE, showWarnings = FALSE)
        
        loc_json_files <- list.files(loc_json_dirs[1], pattern = "*.json", full.names = TRUE)
        file.copy(loc_json_files, loc_data_dir, overwrite = TRUE)
        cat("     Copied", length(loc_json_files), "location JSON files\n")
      }
    }
    
    # Handle provider data
    prov_pattern <- paste0("provider_information_", Sys.Date())
    prov_dirs <- list.dirs(base_cache_dir, full.names = TRUE, recursive = FALSE)
    prov_dirs <- prov_dirs[grepl(prov_pattern, basename(prov_dirs))]
    
    if (length(prov_dirs) > 0) {
      prov_json_dirs <- list.dirs(prov_dirs[1], full.names = TRUE, recursive = FALSE)
      prov_json_dirs <- prov_json_dirs[grepl("provider_jsons_", basename(prov_json_dirs))]
      
      if (length(prov_json_dirs) > 0) {
        cat("   - Copying provider JSONs...\n")
        prov_data_dir <- file.path(data_repo_path, "initial_bulk", "provider")
        dir.create(prov_data_dir, recursive = TRUE, showWarnings = FALSE)
        
        prov_json_files <- list.files(prov_json_dirs[1], pattern = "*.json", full.names = TRUE)
        file.copy(prov_json_files, prov_data_dir, overwrite = TRUE)
        cat("     Copied", length(prov_json_files), "provider JSON files\n")
      }
    }
    
    # Create tarball of initial bulk download
    cat("   - Creating initial bulk tarball...\n")
    bulk_dir <- file.path(data_repo_path, "initial_bulk")
    if (dir.exists(bulk_dir)) {
      tarball_name <- paste0("cqc_initial_bulk_", today_date, ".tar.gz")
      
      system(paste0(
        "cd ", data_repo_path, " && ",
        "tar -czf ", tarball_name, " initial_bulk/"
      ))
      
      cat("     Created", tarball_name, "\n")
      
      # Clean up raw JSON files
      unlink(bulk_dir, recursive = TRUE)
      cat("     Cleaned up raw JSON files\n")
    }
    
  } else {
    cat("5. Skipping JSON copy (not in GitHub Actions or no data repo path)\n")
  }
  
  #-------------------------------------------------------------------------------
  # UPDATE PACKAGE METADATA
  #-------------------------------------------------------------------------------
  cat("6. Updating package metadata...\n")
  
  if (!requireNamespace("desc", quietly = TRUE)) {
    stop("Please install the 'desc' package to write build metadata")
  }
  
  d <- desc::desc(file = "DESCRIPTION")
  d$set("DataBuilt", format(Sys.Date()))
  d$set("Version", "0.1.0")
  d$write()
  
  cat("   - Version set to: 0.1.0\n")
  cat("   - DataBuilt set to:", format(Sys.Date()), "\n")
  
  cat("\n=== First-run build complete ===\n")

}