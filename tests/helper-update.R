if(interactive() || Sys.getenv("GITHUB_ACTIONS") == "true") {
  
  .libPaths(c(Sys.getenv("R_LIBS"), .libPaths()))
  
  library(cqcpack)
  library(desc)
  
  #-------------------------------------------------------------------------------
  # SETUP: Check environment and paths
  #-------------------------------------------------------------------------------
  data_repo_path <- Sys.getenv("CQC_DATA_REPO_PATH", NA_character_)
  is_github_actions <- Sys.getenv("GITHUB_ACTIONS") == "true"
  today_date <- format(Sys.Date(), "%Y-%m-%d")
  
  cat("=== CQC Incremental Update ===\n")
  cat("GitHub Actions:", is_github_actions, "\n")
  cat("Data repo path:", data_repo_path, "\n")
  cat("Date:", today_date, "\n\n")
  
  #-------------------------------------------------------------------------------
  # GETTING INCREMENTAL CHANGES
  #-------------------------------------------------------------------------------
  cat("1. Getting incremental changes...\n")
  changes <- get_incremental_changes()
  
  # Check if there are any changes
  if (is.null(changes) || (length(changes$location_changes) == 0 && length(changes$provider_changes) == 0)) {
    cat("   No changes detected. Exiting.\n")
    if (is_github_actions) {
      quit(save = "no", status = 0)
    } else {
      stop("No changes to process")
    }
  }
  
  cat("   Location changes:", length(changes$location_changes), "\n")
  cat("   Provider changes:", length(changes$provider_changes), "\n\n")
  
  #-------------------------------------------------------------------------------
  # UPDATE DATASETS
  #-------------------------------------------------------------------------------
  cat("2. Updating location dataset...\n")
  update_location_dataset()
  
  cat("3. Updating provider dataset...\n")
  update_provider_dataset()
  
  #-------------------------------------------------------------------------------
  # MERGING DF
  #-------------------------------------------------------------------------------
  cat("4. Merging provider and location data...\n")
  location_df <- build_location_df()
  provider_df <- build_provider_df()
  merged_df <- merge_provider_location()
  cat("   Merged dataset rows:", nrow(merged_df), "\n\n")
  
  cat("4b. Saving updated dataframes to package...\n")
  save(location_df, file = "data/location_df.rda")
  save(provider_df, file = "data/provider_df.rda")
  save(merged_df, file = "data/merged_df.rda")
  cat("   Dataframes saved.\n\n")
  
  #-------------------------------------------------------------------------------
  # COPY CHANGED JSONs TO DATA REPO (if running in GitHub Actions)
  #-------------------------------------------------------------------------------
  if (is_github_actions && !is.na(data_repo_path)) {
    cat("5. Copying changed JSON files to data repository...\n")
    
    # Get the cache directory
    base_cache_dir <- tools::R_user_dir("cqc", "cache")
    
    # Handle location changes
    loc_change_pattern <- paste0("changed_location_information_", Sys.Date())
    loc_change_dirs <- list.dirs(base_cache_dir, full.names = TRUE, recursive = FALSE)
    loc_change_dirs <- loc_change_dirs[grepl(loc_change_pattern, basename(loc_change_dirs))]
    
    loc_json_count <- 0
    if (length(loc_change_dirs) > 0) {
      loc_json_dirs <- list.dirs(loc_change_dirs[1], full.names = TRUE, recursive = FALSE)
      loc_json_dirs <- loc_json_dirs[grepl("location_jsons_", basename(loc_json_dirs))]
      
      if (length(loc_json_dirs) > 0) {
        cat("   - Copying location JSONs...\n")
        loc_data_dir <- file.path(data_repo_path, "incremental", today_date, "location")
        dir.create(loc_data_dir, recursive = TRUE, showWarnings = FALSE)
        
        loc_json_files <- list.files(loc_json_dirs[1], pattern = "*.json", full.names = TRUE)
        file.copy(loc_json_files, loc_data_dir, overwrite = TRUE)
        loc_json_count <- length(loc_json_files)
        cat("     Copied", loc_json_count, "location JSON files\n")
      }
    }
    
    # Handle provider changes
    prov_change_pattern <- paste0("changed_provider_information_", Sys.Date())
    prov_change_dirs <- list.dirs(base_cache_dir, full.names = TRUE, recursive = FALSE)
    prov_change_dirs <- prov_change_dirs[grepl(prov_change_pattern, basename(prov_change_dirs))]
    
    prov_json_count <- 0
    if (length(prov_change_dirs) > 0) {
      prov_json_dirs <- list.dirs(prov_change_dirs[1], full.names = TRUE, recursive = FALSE)
      prov_json_dirs <- prov_json_dirs[grepl("provider_jsons_", basename(prov_json_dirs))]
      
      if (length(prov_json_dirs) > 0) {
        cat("   - Copying provider JSONs...\n")
        prov_data_dir <- file.path(data_repo_path, "incremental", today_date, "provider")
        dir.create(prov_data_dir, recursive = TRUE, showWarnings = FALSE)
        
        prov_json_files <- list.files(prov_json_dirs[1], pattern = "*.json", full.names = TRUE)
        file.copy(prov_json_files, prov_data_dir, overwrite = TRUE)
        prov_json_count <- length(prov_json_files)
        cat("     Copied", prov_json_count, "provider JSON files\n")
      }
    }
    
    # Create tarball of today's incremental changes (only if there are changes)
    if (loc_json_count > 0 || prov_json_count > 0) {
      cat("   - Creating incremental tarball...\n")
      incremental_dir <- file.path(data_repo_path, "incremental", today_date)
      if (dir.exists(incremental_dir)) {
        tarball_name <- paste0("cqc_incremental_", today_date, ".tar.gz")
        
        system(paste0(
          "cd ", shQuote(file.path(data_repo_path, "incremental")), " && ",
          "tar -czf ../", tarball_name, " ", today_date, "/"
        ))
        
        if (file.exists(file.path(data_repo_path, tarball_name))) {
          cat("     Created", tarball_name, "\n")
          
          # Clean up the incremental folder after tarball created
          unlink(incremental_dir, recursive = TRUE)
          cat("     Cleaned up raw JSON files\n")
        }
      }
    } else {
      cat("   - No changes found, skipping tarball creation\n")
    }
    
  } else {
    cat("5. Skipping JSON copy (not in GitHub Actions or no data repo path)\n")
  }
  
  #-------------------------------------------------------------------------------
  # UPDATE PACKAGE METADATA
  #-------------------------------------------------------------------------------
  cat("\n6. Updating package metadata...\n")
  
  if (!requireNamespace("desc", quietly = TRUE)) {
    stop("Please install the 'desc' package to write build metadata")
  }
  
  # Determine package root based on environment
  if (is_github_actions) {
    # In GitHub Actions, we're already in the package root
    desc_file <- "DESCRIPTION"
  } else {
    # For local testing, use find_package_root()
    pkg_root <- find_package_root()
    desc_file <- file.path(pkg_root, "DESCRIPTION")
  }
  
  if (!file.exists(desc_file)) {
    stop("DESCRIPTION file not found at: ", desc_file)
  }
  
  d <- desc::desc(file = desc_file)
  
  # Update date built
  d$set("DataBuilt", format(Sys.Date()))
  
  # Update version (increment patch number)
  current_version <- d$get_version()
  version_parts <- as.numeric(strsplit(as.character(current_version), "\\.")[[1]])
  version_parts[3] <- version_parts[3] + 1
  new_version <- paste(version_parts, collapse = ".")
  
  d$set("Version", new_version)
  d$write()
  
  cat("   - Version updated to:", new_version, "\n")
  cat("   - DataBuilt set to:", format(Sys.Date()), "\n")
  
}