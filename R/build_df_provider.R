#' Build Provider DataFrame from Cached JSON Files
#'
#' Processes cached provider JSON files to create or update a provider dataframe.
#' Requires cache_provider_ids() and cache_provider_jsons() to be run first.
#'
#' @param dataset_name Character. Name for the output dataset. Default: "provider_df"
#' @param update_mode Logical. If TRUE, updates existing dataset with new files only.
#'   If FALSE, processes all files from scratch. Default: TRUE
#'
#' @return A tibble containing provider data. Saved to data/provider_df.rda
#'
#' @export
build_provider_df <- function(dataset_name = "provider_df",
                              update_mode = TRUE) {

  rda_file_path <- file.path("data", paste0(dataset_name, ".rda"))

  # Load existing dataset if in update mode and file exists
  existing_dataset <- NULL
  if (update_mode && file.exists(rda_file_path)) {
    load(rda_file_path)
    existing_dataset <- get(dataset_name)
    message("Loaded existing dataset with ", nrow(existing_dataset), " rows")
  }

  # Get the system cache directory
  base_cache_dir <- tools::R_user_dir("cqc", "cache")
  if (!dir.exists(base_cache_dir)) {
    stop("Cache directory not found: ", base_cache_dir,
         "\nPlease run cache_providerIds() and cache_provider_jsons() first.")
  }

  # Find most recent provider_information folder
  subdirs <- list.dirs(base_cache_dir, full.names = TRUE, recursive = FALSE)
  provider_info_folders <- subdirs[grepl("provider_information_", basename(subdirs))]
  if (length(provider_info_folders) == 0) {
    stop("No provider_information folders found in cache directory: ", base_cache_dir,
         "\nPlease run cache_providerIds() first.")
  }
  most_recent_folder <- sort(provider_info_folders, decreasing = TRUE)[1]
  message("Using folder: ", basename(most_recent_folder))

  # Look for JSON files in the provider_jsons subfolder
  json_subfolders <- list.dirs(most_recent_folder, full.names = TRUE, recursive = FALSE)
  json_subfolder <- json_subfolders[grepl("provider_jsons_", basename(json_subfolders))]
  if (length(json_subfolder) == 0) {
    stop("No provider_jsons folder found in: ", most_recent_folder,
         "\nPlease run cache_provider_jsons() first.")
  }

  # Get all JSON files
  json_files <- list.files(json_subfolder[1], pattern = "^provider_.*\\.json$", full.names = TRUE, recursive = TRUE)
  if (length(json_files) == 0) {
    stop("No provider JSON files found in: ", json_subfolder[1])
  }

  # If in update mode, filter out files that have already been processed
  if (update_mode && !is.null(existing_dataset)) {
    already_processed <- unique(existing_dataset$source_file)
    new_files <- json_files[!basename(json_files) %in% already_processed]

    if (length(new_files) == 0) {
      message("No new files to process. Dataset is up to date.")
      return(invisible(existing_dataset))
    }

    json_files <- new_files
    message("Found ", length(json_files), " new files to process")
  }

  # Extract date from folder name
  folder_name <- basename(most_recent_folder)
  date_extracted <- stringr::str_extract(folder_name, "\\d{4}-\\d{2}-\\d{2}")
  if (is.na(date_extracted)) {
    date_extracted <- folder_name
  }

  message("Processing ", length(json_files), " files from ", folder_name)

  # Process all files with progress
  results_list <- vector("list", length(json_files))
  for (i in seq_along(json_files)) {
    file <- json_files[i]
    # Show progress for each file
    cat("\rProcessed", i, "of", length(json_files), "files")
    flush.console()

    tryCatch({
      # Check if function exists, if not skip for now
      if (exists("extract_provider_row", where = asNamespace("cqcrpack"))) {
        row_data <- cqcrpack::extract_provider_row(file)
      } else {
        # Temporary placeholder - replace with actual function when available
        row_data <- data.frame(
          providerId = basename(file),
          provider_name = "PLACEHOLDER - extract_provider_row not found"
        )
      }
      row_data$folder_date <- date_extracted
      row_data$source_file <- basename(file)
      results_list[[i]] <- row_data
    }, error = function(e) {
      cat("\nError processing", basename(file), ":", e$message, "\n")
      results_list[[i]] <- NULL
    })
  }

  # Final progress update
  cat("\rProcessed", length(json_files), "of", length(json_files), "files\n")

  # Filter out NULL results and combine
  valid_results <- Filter(Negate(is.null), results_list)
  if (length(valid_results) == 0) {
    if (update_mode && !is.null(existing_dataset)) {
      message("No new valid results found, returning existing dataset")
      return(invisible(existing_dataset))
    } else {
      stop("No valid results found. Check that extract_provider_row() function is loaded and working.")
    }
  }

  new_dataset <- dplyr::bind_rows(valid_results)

  # Combine with existing dataset if in update mode
  if (update_mode && !is.null(existing_dataset)) {
    # Remove duplicates based on providerId, keeping the newer data
    dataset <- dplyr::bind_rows(existing_dataset, new_dataset) |>
      dplyr::arrange(desc(folder_date)) |>
      dplyr::distinct(providerId, .keep_all = TRUE) |>
      dplyr::arrange(providerId)

    message("Combined datasets: ", nrow(existing_dataset), " existing + ",
            nrow(new_dataset), " new = ", nrow(dataset), " total rows")
  } else {
    dataset <- new_dataset
  }

  # Only reorder columns if they exist
  if ("folder_date" %in% names(dataset) && "source_file" %in% names(dataset)) {
    dataset <- dataset |> dplyr::select(folder_date, source_file, everything())
  }

  # Save dataset
  if (!dir.exists("data")) dir.create("data", recursive = TRUE)
  assign(dataset_name, dataset)
  save(list = dataset_name, file = rda_file_path, compress = "bzip2")

  message("Saved ", nrow(dataset), " rows to ", rda_file_path)

  # Display cache location info
  message("Data source: ", json_subfolder[1])

  invisible(dataset)
}

#-------------------------------------------------------------------------------