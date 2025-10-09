#' Get CQC Changes and Cache to System Directory
#'
#' @param startDate Start date in DD.MM.YYYY format
#' @param endDate End date in DD.MM.YYYY format
#' @param api_key API key (defaults to environment variable)
#' @param refresh_cache Logical indicating whether to refresh cache even if file exists
#' @return Data frame with location_changes and provider_changes columns
#' @export
get_cqc_changes <- function(startDate,
                            endDate,
                            api_key = get_api_key(),
                            refresh_cache = FALSE) {

  # Date validation and conversion
  sd <- as.Date(startDate, format = "%d.%m.%Y")
  ed <- as.Date(endDate, format = "%d.%m.%Y")
  if (is.na(sd) || is.na(ed)) {
    stop("startDate/endDate must be in DD.MM.YYYY format.")
  }

  # Get the system cache directory (same pattern as location functions)
  base_cache_dir <- tools::R_user_dir("cqc", "cache")

  # Create the changes_information directory with today's date
  cache_dir <- file.path(
    base_cache_dir,
    paste0("changes_information_", Sys.Date())
  )

  # Create the cache directory if it doesn't exist
  if (!dir.exists(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE)
    message("Created cache directory: ", cache_dir)
  } else {
    message("Cache directory already exists: ", cache_dir)
  }

  # Create cache file name based on date range
  date_range <- paste0(format(sd, "%Y%m%d"), "_to_", format(ed, "%Y%m%d"))
  cache_file <- file.path(cache_dir, paste0("changes_", date_range, ".rds"))

  # Load from cache if file exists and user doesn't want to refresh
  if (!refresh_cache && file.exists(cache_file)) {
    message("Loading cached changes from ", cache_file)
    return(readRDS(cache_file))
  }

  # If no cache or refresh requested, fetch data from API
  message("Fetching changes from API...")

  # Timestamp formatting
  start_ts <- paste0(format(sd, "%Y-%m-%d"), "T00:00:00Z")
  end_ts <- paste0(format(ed, "%Y-%m-%d"), "T23:59:59Z")

  # API key validation
  if (api_key == "") {
    stop("API key not found. Please set R_CQC_API_KEY environment variable.")
  }

  # HTTP headers setup
  hdrs <- set_headers(api_key)

  # Helper function to get changes for a specific type
  get_changes_by_type <- function(change_type) {
    endpoint <- paste0("https://api.service.cqc.org.uk/public/v1/changes/", change_type)

    res <- httr::GET(
      endpoint,
      hdrs,
      query = list(
        startTimestamp = start_ts,
        endTimestamp = end_ts
      )
    )

    if (httr::status_code(res) != 200) {
      stop(
        "Request failed for ", change_type, ": HTTP ", httr::status_code(res), "\n",
        httr::content(res, "text", encoding = "UTF-8")
      )
    }

    parsed <- jsonlite::fromJSON(
      httr::content(res, "text", encoding = "UTF-8"),
      flatten = TRUE
    )

    return(parsed$changes)
  }

  # Get both location and provider changes
  location_changes <- get_changes_by_type("location")
  provider_changes <- get_changes_by_type("provider")

  # Handle empty results
  if (is.null(location_changes)) location_changes <- character(0)
  if (is.null(provider_changes)) provider_changes <- character(0)

  # Create data frame with both columns
  max_length <- max(length(location_changes), length(provider_changes))

  # Pad shorter vector with NAs
  if (length(location_changes) < max_length) {
    location_changes <- c(location_changes, rep(NA, max_length - length(location_changes)))
  }
  if (length(provider_changes) < max_length) {
    provider_changes <- c(provider_changes, rep(NA, max_length - length(provider_changes)))
  }

  # Create result data frame
  result <- data.frame(
    location_changes = location_changes,
    provider_changes = provider_changes,
    stringsAsFactors = FALSE
  )

  # Save to cache
  saveRDS(result, cache_file)
  message("Cached changes data saved to ", cache_file)
  message("Changes stored at: ", cache_file)

  return(result)
}

#-------------------------------------------------------------------------------
#' Get Incremental Changes and Cache to System Directory
#'
#' @param dataset_file Dataset file name (default: "location_df.rda")
#' @param data_folder Path to data folder (default: "data")
#' @param date_column Column containing date information (default: "folder_date")
#' @param api_key API key (defaults to get_api_key())
#' @return Data frame with location_changes and provider_changes columns
#' @export
get_incremental_changes <- function(
    dataset_file = "location_df.rda",
    data_folder = "data",
    date_column = "folder_date",
    api_key = get_api_key()
) {

  # Construct full file path
  file_path <- file.path(data_folder, dataset_file)

  # Check if data folder exists
  if (!dir.exists(data_folder)) {
    stop("Data folder not found: ", data_folder, "\n",
         "Current working directory: ", getwd())
  }

  # Check if file exists
  if (!file.exists(file_path)) {
    files_in_folder <- list.files(data_folder, full.names = FALSE)
    stop("Dataset file not found: ", file_path, "\n",
         "Available files in ", data_folder, ": ", paste(files_in_folder, collapse = ", "))
  }

  # Read the dataset (always RDA format)
  cat("Reading dataset from:", file_path, "\n")

  # Load the RDA file - this loads objects into the environment
  env <- new.env()
  load(file_path, envir = env)

  # Get the location_df object (assuming it's named location_df in the RDA file)
  if ("location_df" %in% names(env)) {
    location_df <- env$location_df
  } else {
    # If not named location_df, try to get the first object
    obj_names <- names(env)
    if (length(obj_names) > 0) {
      location_df <- env[[obj_names[1]]]
      cat("Using object:", obj_names[1], "from RDA file\n")
    } else {
      stop("No objects found in RDA file: ", file_path)
    }
  }

  # Check if specified date column exists
  if (!date_column %in% names(location_df)) {
    stop("Date column '", date_column, "' not found in dataset. Available columns: ",
         paste(names(location_df), collapse = ", "))
  }

  cat("Using date column:", date_column, "\n")

  # Extract and convert dates
  date_values <- location_df[[date_column]]

  # Handle different date formats
  date_values <- as.Date(date_values)

  # Remove any NA dates
  valid_dates <- date_values[!is.na(date_values)]

  if (length(valid_dates) == 0) {
    stop("No valid dates found in column '", date_column, "'.")
  }

  # Find the latest date
  latest_date <- max(valid_dates)
  cat("Latest date found in dataset:", as.character(latest_date), "\n")

  # Get current system date
  current_date <- Sys.Date()
  cat("Current system date:", as.character(current_date), "\n")

  # Check if we need to get any changes
  if (latest_date >= current_date) {
    cat("Dataset is already up to date. No changes to retrieve.\n")
    return(data.frame(location_changes = character(0), provider_changes = character(0)))
  }

  # Format dates for the API (DD.MM.YYYY format required by get_cqc_changes)
  start_date_formatted <- format(latest_date, "%d.%m.%Y")
  end_date_formatted <- format(current_date, "%d.%m.%Y")

  cat("Retrieving changes from", start_date_formatted, "to", end_date_formatted, "\n")

  # Get the incremental changes using our existing function
  incremental_changes <- get_cqc_changes(
    startDate = start_date_formatted,
    endDate = end_date_formatted,
    api_key = api_key
  )

  # Summary information
  location_count <- sum(!is.na(incremental_changes$location_changes))
  provider_count <- sum(!is.na(incremental_changes$provider_changes))

  cat("Retrieved", location_count, "location changes and", provider_count, "provider changes.\n")

  # Save changes to cache directory using consistent structure
  if (location_count > 0 || provider_count > 0) {
    # Get the system cache directory (same as other cache functions)
    base_cache_dir <- tools::R_user_dir("cqc", "cache")

    # Save location changes and cache JSONs
    if (location_count > 0) {
      # Create changed location information directory with today's date
      location_cache_dir <- file.path(
        base_cache_dir,
        paste0("changed_location_information_", Sys.Date())
      )

      # Create JSON subdirectory within changed location information
      location_json_dir <- file.path(
        location_cache_dir,
        paste0("location_jsons_", Sys.Date())
      )

      # Create directories if they don't exist
      if (!dir.exists(location_json_dir)) {
        dir.create(location_json_dir, recursive = TRUE)
        message("Created location cache directory: ", location_json_dir)
      }

      location_ids <- incremental_changes$location_changes[!is.na(incremental_changes$location_changes)]

      # Save location IDs to the main cache directory
      location_file <- file.path(location_cache_dir, paste0("changed_location_ids_", Sys.Date(), ".rds"))
      saveRDS(location_ids, location_file)
      cat("Saved", length(location_ids), "location IDs to:", location_file, "\n")

      # Cache location JSONs to JSON subdirectory
      cat("Caching location JSONs to cache directory...\n")
      cache_location_jsons(
        api_key = api_key,
        location_ids = location_ids,
        output_dir = location_json_dir
      )
    }

    # Save provider changes and cache JSONs
    if (provider_count > 0) {
      # Create changed provider information directory with today's date
      provider_cache_dir <- file.path(
        base_cache_dir,
        paste0("changed_provider_information_", Sys.Date())
      )

      # Create JSON subdirectory within changed provider information
      provider_json_dir <- file.path(
        provider_cache_dir,
        paste0("provider_jsons_", Sys.Date())
      )

      # Create directories if they don't exist
      if (!dir.exists(provider_json_dir)) {
        dir.create(provider_json_dir, recursive = TRUE)
        message("Created provider cache directory: ", provider_json_dir)
      }

      provider_ids <- incremental_changes$provider_changes[!is.na(incremental_changes$provider_changes)]

      # Save provider IDs to the main cache directory
      provider_file <- file.path(provider_cache_dir, paste0("changed_provider_ids_", Sys.Date(), ".rds"))
      saveRDS(provider_ids, provider_file)
      cat("Saved", length(provider_ids), "provider IDs to:", provider_file, "\n")

      # Cache provider JSONs to JSON subdirectory
      cat("Caching provider JSONs to cache directory...\n")
      cache_provider_jsons(
        api_key = api_key,
        provider_ids = provider_ids,
        output_dir = provider_json_dir
      )
    }
  }

  return(incremental_changes)
}
#-------------------------------------------------------------------------------
