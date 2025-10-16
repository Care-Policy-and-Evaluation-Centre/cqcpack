
if(interactive() || Sys.getenv("GITHUB_ACTIONS") == "true") { # Dev-only file, not for R CMD CHECK
  
  .libPaths(c(Sys.getenv("R_LIBS"), .libPaths()))

  library(cqcpack)
  
  #-------------------------------------------------------------------------------
  # SETUP: Check environment and paths
  #-------------------------------------------------------------------------------
  data_repo_path <- Sys.getenv("CQC_DATA_REPO_PATH", NA_character_)
  is_github_actions <- Sys.getenv("GITHUB_ACTIONS") == "true"
  
  cat("=== CQC Incremental Update ===\n")
  cat("GitHub Actions:", is_github_actions, "\n")
  cat("Data repo path:", data_repo_path, "\n\n")
  
  #-------------------------------------------------------------------------------
  # GETTING INCREMENTAL CHANGES
  #-------------------------------------------------------------------------------
  cat("1. Getting incremental changes...\n")
  changes <- get_incremental_changes()
  
  cat("2. Updating location dataset...\n")
  update_location_dataset()
  
  cat("3. Updating provider dataset...\n")
  update_provider_dataset()
  
  #-------------------------------------------------------------------------------
  # MERGING DF
  #-------------------------------------------------------------------------------
  cat("4. Merging provider and location data...\n")
  merged_data <- merge_provider_location()
  
  
  #-------------------------------------------------------------------------------
  # COPY CHANGED JSONs TO DATA REPO (if running in GitHub Actions)
  #-------------------------------------------------------------------------------
  if (is_github_actions && !is.na(data_repo_path)) {
    cat("5. Copying changed JSON files to data repository...\n")
    
    # Get the cache directory
    base_cache_dir <- tools::R_user_dir("cqc", "cache")
    today_date <- format(Sys.Date(), "%Y-%m-%d")
    
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
          "cd ", file.path(data_repo_path, "incremental"), " && ",
          "tar -czf ../", tarball_name, " ", today_date, "/"
        ))
        
        cat("     Created", tarball_name, "\n")
        
        # Clean up the incremental folder after tarball created
        unlink(incremental_dir, recursive = TRUE)
        cat("     Cleaned up raw JSON files\n")
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
  cat("6. Updating package metadata...\n")
  
  if (!requireNamespace("desc", quietly = TRUE)) {
    stop("Please install the 'desc' package to write build metadata")
  }
  
  d <- desc::desc(file = "DESCRIPTION")
  
  # Update date built
  d$set("DataBuilt", format(Sys.Date()))
  
  # Update version (increment patch number)
  version <- d$get("Version") |>
    strsplit(".", fixed = TRUE) |>
    unname() |>
    unlist() |>
    as.integer()
  minor_num_idx <- length(version)
  version[minor_num_idx] <- version[minor_num_idx] + 1
  new_version <- version |>
    as.character() |>
    paste0(collapse = ".")
  d$set("Version", new_version)
  d$write()
  
  cat("   - Version updated to:", new_version, "\n")
  cat("   - DataBuilt set to:", format(Sys.Date()), "\n")
  
  #-------------------------------------------------------------------------------
  # ARCHIVE OLD PACKAGE VERSIONS
  #-------------------------------------------------------------------------------
  cat("7. Archiving old package versions...\n")
  
  old_versions <- list.files("../", pattern = "^cqcpack_.+\\.tar\\.gz$", full.names = TRUE)
  
  if (length(old_versions) > 0) {
    for (old_version in old_versions) {
      archive_dir <- "../archive"
      if (!dir.exists(archive_dir)) dir.create(archive_dir, recursive = TRUE)
      
      file.copy(
        old_version,
        file.path(archive_dir, basename(old_version)),
        overwrite = TRUE
      )
      unlink(old_version)
      cat("   - Archived:", basename(old_version), "\n")
    }
  } else {
    cat("   - No old versions to archive\n")
  }
  
  #-------------------------------------------------------------------------------
  # BUILD PACKAGE
  #-------------------------------------------------------------------------------
  cat("8. Building package tar.gz...\n")
  devtools::build()
  
  cat("\n=== Incremental update complete ===\n")
  
}

