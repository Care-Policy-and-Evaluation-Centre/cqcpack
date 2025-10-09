#' Update Provider Dataset with Incremental Changes
#'
#' @param data_folder Path to data folder (default: "data")
#' @param dataset_file Dataset file name (default: "provider_df.rda")
#' @param date_column Column containing date information (default: "folder_date")
#' @return Invisibly returns summary information
#' @export
update_provider_dataset <- function(
    data_folder = "data",
    dataset_file = "provider_df.rda",
    date_column = "folder_date"
) {

  # Get the system cache directory (consistent with other cache functions)
  base_cache_dir <- tools::R_user_dir("cqc", "cache")

  if (!dir.exists(base_cache_dir)) {
    stop("Cache directory not found: ", base_cache_dir,
         "\nPlease run get_incremental_changes() first.")
  }

  # Find the latest changed_provider_information folder
  pattern <- "changed_provider_information_"
  folders <- list.dirs(base_cache_dir, full.names = TRUE, recursive = FALSE)
  change_folders <- folders[grepl(pattern, basename(folders))]

  if (length(change_folders) == 0) {
    stop("No changed_provider_information folders found in: ", base_cache_dir,
         "\nPlease run get_incremental_changes() first.")
  }

  # Get the latest folder (by date in folder name)
  latest_folder <- sort(change_folders, decreasing = TRUE)[1]
  cat("Using latest changes from:", basename(latest_folder), "\n")

  # Define cache folder paths using consistent structure
  provider_json_subfolders <- list.dirs(latest_folder, full.names = TRUE, recursive = FALSE)
  provider_json_folder <- provider_json_subfolders[grepl("provider_jsons_", basename(provider_json_subfolders))]

  if (length(provider_json_folder) == 0) {
    stop("No provider_jsons folder found in: ", latest_folder)
  }

  provider_cache_path <- provider_json_folder[1]

  # Load current dataset
  dataset_path <- file.path(data_folder, dataset_file)
  if (!file.exists(dataset_path)) {
    stop("Dataset file not found: ", dataset_path)
  }

  cat("Loading current dataset from:", dataset_path, "\n")
  env <- new.env()
  load(dataset_path, envir = env)

  # Get the provider_df object
  if ("provider_df" %in% names(env)) {
    current_df <- env$provider_df
  } else {
    obj_names <- names(env)
    if (length(obj_names) > 0) {
      current_df <- env[[obj_names[1]]]
      cat("Using object:", obj_names[1], "from dataset\n")
    } else {
      stop("No objects found in dataset file")
    }
  }

  # Process provider JSONs
  cat("Processing provider JSONs from:", provider_cache_path, "\n")
  provider_json_files <- list.files(provider_cache_path, pattern = "^provider_.*\\.json$", full.names = TRUE)

  if (length(provider_json_files) == 0) {
    cat("No provider JSON files found in cache\n")
    updated_df <- current_df
  } else {
    cat("Found", length(provider_json_files), "provider JSON files\n")

    # Process each provider JSON file
    new_provider_records <- list()
    current_date <- format(Sys.Date(), "%Y-%m-%d")

    for (i in seq_along(provider_json_files)) {
      json_file <- provider_json_files[i]

      tryCatch({
        # Extract provider data from JSON (assumes you have extract_provider_row function)
        if (exists("extract_provider_row")) {
          provider_record <- extract_provider_row(json_file)
          provider_record[[date_column]] <- current_date
          provider_record$source_file <- basename(json_file)
          new_provider_records[[i]] <- provider_record
        } else {
          # Alternative: basic JSON reading if extract_provider_row doesn't exist
          json_data <- jsonlite::fromJSON(json_file, flatten = TRUE)
          # Add basic processing here if needed
          cat("Processed:", basename(json_file), "\n")
        }
      }, error = function(e) {
        cat("Error processing", basename(json_file), ":", e$message, "\n")
      })
    }

    # Combine new provider records
    if (length(new_provider_records) > 0 && exists("extract_provider_row")) {
      new_providers_df <- do.call(rbind, Filter(Negate(is.null), new_provider_records))

      # Remove existing records for updated providers (if providerId column exists)
      if ("providerId" %in% names(current_df) && "providerId" %in% names(new_providers_df)) {
        updated_provider_ids <- new_providers_df$providerId
        current_df <- current_df[!current_df$providerId %in% updated_provider_ids, ]
        cat("Removed", length(updated_provider_ids), "existing provider records for update\n")
      }

      # Add new records
      updated_df <- rbind(current_df, new_providers_df)
      cat("Added", nrow(new_providers_df), "new provider records\n")
    } else {
      updated_df <- current_df
      cat("No provider records processed\n")
    }
  }

  # Process location JSONs (check for location changes that might affect providers)
  location_change_folders <- folders[grepl("changed_location_information_", basename(folders))]

  if (length(location_change_folders) > 0) {
    latest_location_folder <- sort(location_change_folders, decreasing = TRUE)[1]
    cat("Checking for related location changes in:", basename(latest_location_folder), "\n")

    # Find location JSON subfolder
    location_json_subfolders <- list.dirs(latest_location_folder, full.names = TRUE, recursive = FALSE)
    location_json_folder <- location_json_subfolders[grepl("location_jsons_", basename(location_json_subfolders))]

    if (length(location_json_folder) > 0) {
      location_cache_path <- location_json_folder[1]
      cat("Found related location changes in:", location_cache_path, "\n")

      location_json_files <- list.files(location_cache_path, pattern = "^location_.*\\.json$", full.names = TRUE)

      if (length(location_json_files) > 0) {
        cat("Found", length(location_json_files), "related location JSON files\n")
        # Note: Processing would depend on business logic for how location changes affect providers
      }
    }
  }

  # Reorder columns if folder_date exists
  if (date_column %in% names(updated_df)) {
    # Move date column and source_file to front
    other_cols <- setdiff(names(updated_df), c(date_column, "source_file"))
    if ("source_file" %in% names(updated_df)) {
      updated_df <- updated_df[, c(date_column, "source_file", other_cols)]
    } else {
      updated_df <- updated_df[, c(date_column, other_cols)]
    }
  }

  # Save updated dataset
  provider_df <- updated_df
  save(provider_df, file = dataset_path, compress = "bzip2")

  cat("Dataset updated successfully!\n")
  cat("Total rows in dataset:", nrow(updated_df), "\n")

  if (date_column %in% names(updated_df)) {
    current_date_count <- sum(updated_df[[date_column]] == current_date, na.rm = TRUE)
    cat("Rows with today's date (", current_date, "):", current_date_count, "\n")
  }

  # Return summary information
  invisible(list(
    dataset_path = dataset_path,
    total_rows = nrow(updated_df),
    latest_folder = latest_folder,
    provider_jsons_processed = length(provider_json_files)
  ))
}

#-------------------------------------------------------------------------------
