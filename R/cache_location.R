#' Cache Location IDs to System Cache
#'
#' Retrieves all location data from the CQC API (using scrape_locations()),
#' extracts the location IDs, and saves them to the system cache directory.
#'
#' @param api_key A character string containing your API key.
#' @param refresh_cache Logical indicating whether to refresh the cache even
#'                      if an RDS file exists.
#' @return Invisibly returns the vector of location IDs.
#' @export
cache_location_ids <- function(api_key = get_api_key(),
                               refresh_cache = FALSE) {

  # Get the system cache directory
  base_cache_dir <- tools::R_user_dir("cqc", "cache")

  # Create the location_information directory with today's date
  cache_dir <- file.path(
    base_cache_dir,
    paste0("location_information_", Sys.Date())
  )

  # Create the cache directory if it doesn't exist, otherwise notify the user
  if (!dir.exists(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE)
    message("Created cache directory: ", cache_dir)
  } else {
    message("Cache directory already exists: ", cache_dir)
  }

  # Define the full path to the RDS file with date appended
  cache_file <- file.path(cache_dir, paste0("location_ids_", Sys.Date(), ".rds"))

  # Load from cache if the file exists and user doesn't want to refresh
  if (!refresh_cache && file.exists(cache_file)) {

    message("Loading cached location IDs from ", cache_file)
    location_data <- readRDS(cache_file)
  } else {
    
    # If no cache or refresh requested, fetch data from API
    message("Scraping locations from API...")
    location_data <- scrape_locations(api_key, per_page = 100)

    # Fail if data couldn't be retrieved
    if (nrow(location_data) == 0) {
      stop("Something has gone wrong. Failed to return location data. Check your API key and internet connection.")
    }

    #! debug
    message("This is location data")
    print(head(location_data))
    #! end debug

    # Save the location data to the cache
    saveRDS(location_data, cache_file)
    message("Cached location data saved to ", cache_file)
  }

  # Extract location IDs from the loaded or freshly scraped data
  if ("locationId" %in% names(location_data)) {
    location_ids <- location_data$locationId
  } else {
    stop("The RDS file does not contain a valid 'locationId' column.")
  }

  # Display the final storage location
  message("Location IDs stored at: ", cache_file)

  # Return location IDs invisibly
  invisible(location_ids)
}

#-------------------------------------------------------------------------------

#' Cache Location JSON Files from the CQC API to System Cache
#'
#' Downloads detailed JSON records for a list of location IDs from the CQC API
#' and saves them to the system cache directory, showing an
#' in-place progress counter as it goes.
#'
#' @param api_key A character string containing your API key.
#' @param location_ids Optional vector of location IDs. If NULL, read from cached RDS file.
#' @param output_dir Directory to save JSON files. Defaults to system cache location.
#' @param pause_time Seconds to wait between API requests to avoid rate limiting.
#' @param overwrite Whether to overwrite existing JSON files. Default is FALSE.
#' @return Invisibly returns a named list of saved file paths.
#' @export
cache_location_jsons <- function(api_key = get_api_key(),
                                 location_ids = NULL,
                                 output_dir = NULL,
                                 pause_time = 0.02,
                                 overwrite = FALSE) {

  # Hardcoded RDS file name (follows standard pattern from cache_location_ids)
  id_rds_file <- paste0("location_ids_", Sys.Date(), ".rds")

  # Get the system cache directory (same as location_ids function)
  base_cache_dir <- tools::R_user_dir("cqc", "cache")

  # Use the location_information directory with today's date
  location_info_dir <- file.path(
    base_cache_dir,
    paste0("location_information_", Sys.Date())
  )

  # Create JSON subdirectory within location_information
  json_output_dir <- file.path(
    location_info_dir,
    paste0("location_jsons_", Sys.Date())
  )

  # Determine output directory under system cache
  if (is.null(output_dir)) {
    output_dir <- json_output_dir
  }

  # Helper to clean up if no files end up downloaded
  destroy_output_dir <- function(output_dir) {
    files <- list.files(output_dir)
    if (length(files) == 0) {
      unlink(output_dir, recursive = TRUE)
      message("Function could not download any JSON files. Destroying output directory.")
    }
  }
  on.exit(destroy_output_dir(output_dir))

  # Create output directory if it doesn't exist
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    message("Created JSON cache directory: ", output_dir)
  }

  # Load location IDs if not passed in
  if (is.null(location_ids)) {
    # Look for RDS file in the location_information directory
    id_rds_full_path <- file.path(location_info_dir, id_rds_file)

    if (!file.exists(id_rds_full_path)) {
      stop("RDS file not found at: ", id_rds_full_path,
           "\nPlease run cache_location_ids() first or provide location_ids directly.")
    }

    location_data <- readRDS(id_rds_full_path)
    message("Loaded location IDs from: ", id_rds_full_path)

    if (is.data.frame(location_data) && "locationId" %in% names(location_data)) {
      location_ids <- location_data$locationId
    } else if (is.vector(location_data)) {
      location_ids <- location_data
    } else {
      stop("The RDS file does not contain a valid set of location IDs.")
    }
  }

  n_ids <- length(location_ids)
  if (n_ids == 0) stop("No location IDs to process.")

  # Prepare headers
  headers <- set_headers(api_key)
  saved_files <- vector("list", length = n_ids) |> setNames(location_ids)
  
  # Track IDs that encounter 504 errors
  error_504_ids <- character(0)

  # Loop with in-place progress counter
  for (i in seq_along(location_ids)) {
    id <- location_ids[i]

    # Overwrite the same console line with progress
    cat(sprintf("\rDownloading JSON %d/%d (ID: %s)", i, n_ids, id))
    flush.console()

    # Fetch JSON
    url <- paste0("https://api.service.cqc.org.uk/public/v1/locations/", id)
    res <- httr::GET(url, headers)

    if (httr::http_error(res)) {
      status_code <- httr::status_code(res)
      
      # Check specifically for 504 Gateway Timeout errors
      if (status_code == 504) {
        error_504_ids <- c(error_504_ids, id)
      }
      
      warning("Error on ID ", id, ": ", httr::http_status(res)$message)
      next
    }

    json_text <- httr::content(res, as = "text", encoding = "UTF-8")

    # Save or skip
    out_file <- file.path(output_dir, paste0("location_", id, ".json"))
    if (!overwrite && file.exists(out_file)) {
      # do nothing
    } else {
      writeLines(json_text, out_file)
    }

    saved_files[[id]] <- out_file
    Sys.sleep(pause_time)
  }

  # Done—print a newline to finish the progress line
  cat("\n")

  # Display the final storage location
  message("Location JSON files stored in: ", output_dir)
  
  # Report any 504 errors encountered
  if (length(error_504_ids) > 0) {
    message("The following location IDs encountered 504 Gateway Timeout errors:")
    message(paste(error_504_ids, collapse = ", "))
  }

  invisible(saved_files)
}

#-------------------------------------------------------------------------------
