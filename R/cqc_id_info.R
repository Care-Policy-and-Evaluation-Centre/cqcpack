#' Get CQC ID Information
#'
#' Retrieves information for CQC location or provider IDs. By default returns
#' comprehensive data including both location and provider information from the
#' merged_df which contains both location IDs and provider IDs.
#'
#' @details This function uses dplyr internally but does not require users to load it.
#'
#' @param ids Character vector of CQC IDs to look up (all must be same type)
#' @param id_type Character. Must be either "location" or "provider" to specify ID type
#' @param package_path Character. Path to package data folder. Default: "."
#' @param merge Logical. If TRUE (default), uses merged_df for comprehensive data.
#'   If FALSE, uses location_df or provider_df for specific data only.
#'
#' @return A tibble with the requested information. Empty tibble if no matches found.
#'
#' @export
get_cqc_id_info <- function(ids, id_type, package_path = ".", merge = TRUE) {

  # Validate id_type parameter
  if (missing(id_type)) {
    stop("Please specify id_type as either 'location' or 'provider'")
  }

  id_type <- tolower(id_type)
  if (!id_type %in% c("location", "provider")) {
    stop("id_type must be either 'location' or 'provider'")
  }

  # Validate that IDs were provided
  if (missing(ids) || length(ids) == 0) {
    stop("Please provide at least one ID")
  }

  # Convert to character vector if needed
  ids <- as.character(ids)

  # Check if any of the three possible data frames are already loaded in the global environment
  # This helps avoid reloading data in case the user has already loaded it before
  merged_in_env <- exists("merged_df", envir = .GlobalEnv)
  location_in_env <- exists("location_df", envir = .GlobalEnv)
  provider_in_env <- exists("provider_df", envir = .GlobalEnv)

  # If data exists in environment, use it directly
  if (merged_in_env || location_in_env || provider_in_env) {
    df_loaded <- NULL
    df_name <- ""

    # PRIORITY 1: Use merged_df if merge=TRUE and it exists
    if (merge && merged_in_env) {
      cat("Using merged_df (already loaded in environment) - contains both location and provider data\n")
      df_loaded <- get("merged_df", envir = .GlobalEnv)
      df_name <- "merged_df"
      
    # PRIORITY 2: Use specific df if merge=FALSE  
    } else if (!merge && id_type == "location" && location_in_env) {
      cat("Using location_df (merge=FALSE) - contains only location data\n")
      df_loaded <- get("location_df", envir = .GlobalEnv)
      df_name <- "location_df"
      
    } else if (!merge && id_type == "provider" && provider_in_env) {
      cat("Using provider_df (merge=FALSE) - contains only provider data\n")
      df_loaded <- get("provider_df", envir = .GlobalEnv)
      df_name <- "provider_df"
      
    # FALLBACK: Use what's available
    } else if (merged_in_env) {
      cat("Using merged_df (fallback option)\n")
      df_loaded <- get("merged_df", envir = .GlobalEnv)
      df_name <- "merged_df"
    } else if (id_type == "location" && location_in_env) {
      cat("Using location_df (only available option for location IDs)\n")
      df_loaded <- get("location_df", envir = .GlobalEnv)
      df_name <- "location_df"
    } else if (id_type == "provider" && provider_in_env) {
      cat("Using provider_df (only available option for provider IDs)\n")
      df_loaded <- get("provider_df", envir = .GlobalEnv)
      df_name <- "provider_df"
    } else {
      stop("Cannot find appropriate dataframe for your request")
    }

  } else {
    # If not in environment, try loading from files
    cat("Data not found in environment, checking files...\n")

    # Construct relative path to data directory
    data_dir <- file.path(package_path, "data")

    # Define file paths
    merged_df_path <- file.path(data_dir, "merged_df.rda")
    merged_provider_location_path <- file.path(data_dir, "merged_provider_location.rda")
    location_df_path <- file.path(data_dir, "location_df.rda")
    provider_df_path <- file.path(data_dir, "provider_df.rda")

    # Check which files exist
    merged_exists <- file.exists(merged_df_path)
    merged_alt_exists <- file.exists(merged_provider_location_path)
    location_exists <- file.exists(location_df_path)
    provider_exists <- file.exists(provider_df_path)

    # Helper function to load data from .rda file
    load_data_file <- function(file_path) {
      env <- new.env()
      load(file_path, envir = env)
      # Return the object in the newly created environment
      return(get(ls(env)[1], envir = env))
    }

    # Load appropriate dataframe based on logic
    df_loaded <- NULL
    df_name <- ""

    # Respect merge parameter and prioritize merged data
    if (merge && merged_exists) {
      cat("Using merged_df (merge=TRUE) - contains both location and provider data\n")
      df_loaded <- load_data_file(merged_df_path)
      df_name <- "merged_df"
      
    } else if (merge && merged_alt_exists) {
      cat("Using merged_provider_location.rda (merge=TRUE) - contains both location and provider data\n")
      df_loaded <- load_data_file(merged_provider_location_path)
      df_name <- "merged_df"

    } else if (!merge && id_type == "location" && location_exists) {
      cat("Using location_df (merge=FALSE) - contains only location data\n")
      df_loaded <- load_data_file(location_df_path)
      df_name <- "location_df"

    } else if (!merge && id_type == "provider" && provider_exists) {
      cat("Using provider_df (merge=FALSE) - contains only provider data\n")
      df_loaded <- load_data_file(provider_df_path)
      df_name <- "provider_df"

    } else if (merged_exists) {
      cat("Using merged_df (fallback option)\n")
      df_loaded <- load_data_file(merged_df_path)
      df_name <- "merged_df"
      
    } else if (merged_alt_exists) {
      cat("Using merged_provider_location.rda (fallback option)\n")
      df_loaded <- load_data_file(merged_provider_location_path)
      df_name <- "merged_df"

    } else if (id_type == "location" && location_exists) {
      cat("Using location_df (only available option for location IDs)\n")
      df_loaded <- load_data_file(location_df_path)
      df_name <- "location_df"

    } else if (id_type == "provider" && provider_exists) {
      cat("Using provider_df (only available option for provider IDs)\n")
      df_loaded <- load_data_file(provider_df_path)
      df_name <- "provider_df"

    } else {
      stop("No suitable data files found in the specified path")
    }
  }

  # Convert to tibble if not already
  df_loaded <- dplyr::as_tibble(df_loaded)

  # Determine which ID column to use based on id_type parameter
  search_column <- NULL

  if (df_name == "merged_df") {
    # In merged_df, select the appropriate column based on id_type
    if (id_type == "location") {
      # Check for location ID columns
      if ("location_locationId" %in% names(df_loaded)) {
        search_column <- "location_locationId"
      } else if ("locationId" %in% names(df_loaded)) {
        search_column <- "locationId"
      } else {
        stop("No location ID column found in merged_df")
      }
    } else {  # id_type == "provider"
      # Check for provider ID columns
      if ("location_providerId" %in% names(df_loaded)) {
        search_column <- "location_providerId"
      } else if ("provider_providerId" %in% names(df_loaded)) {
        search_column <- "provider_providerId"
      } else if ("providerId" %in% names(df_loaded)) {
        search_column <- "providerId"
      } else {
        stop("No provider ID column found in merged_df")
      }
    }

  } else if (df_name == "location_df") {
    # Validate that user is searching for location IDs
    if (id_type != "location") {
      stop("Cannot search for provider IDs in location_df. Please set merge=TRUE or load provider data.")
    }

    # For location_df, use locationId
    if ("locationId" %in% names(df_loaded)) {
      search_column <- "locationId"
    } else if ("location_locationId" %in% names(df_loaded)) {
      search_column <- "location_locationId"
    } else {
      stop("location_df should contain locationId column")
    }

  } else if (df_name == "provider_df") {
    # Validate that user is searching for provider IDs
    if (id_type != "provider") {
      stop("Cannot search for location IDs in provider_df. Please set merge=TRUE or load location data.")
    }

    # For provider_df, use providerId
    if ("providerId" %in% names(df_loaded)) {
      search_column <- "providerId"
    } else if ("provider_providerId" %in% names(df_loaded)) {
      search_column <- "provider_providerId"
    } else {
      stop("provider_df should contain providerId column")
    }
  }

  cat("Searching for", id_type, "IDs in column:", search_column, "\n")

  # Look up all IDs at once using vectorized filtering (using dplyr:: prefix)
  results <- dplyr::filter(df_loaded, .data[[search_column]] %in% ids)

  # Identify which IDs were found and which were missing
  found_ids <- unique(results[[search_column]])
  missing_ids <- setdiff(ids, found_ids)

  # Issue warnings for missing IDs
  if (length(missing_ids) > 0) {
    warning("The following ", id_type, " ID(s) were not found: ",
            paste(missing_ids, collapse = ", "))
  }

  # Summary message
  cat("\nFound information for", length(found_ids), "out of", length(ids),
      "requested", id_type, "ID(s)\n")

  if (nrow(results) == 0) {
    cat("No matching records found\n")
    return(dplyr::tibble())
  }

  return(results)
}