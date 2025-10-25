#' Merge Provider and Location Data and Save to Package
#'
#' @param join_fn Join function to use (default: dplyr::inner_join)
#' @return Invisibly returns the merged data frame
#' @export
merge_provider_location <- function(join_fn = dplyr::inner_join) {
  
  # Determine package root and data directory
  pkg_root <- find_package_root()
  data_dir <- file.path(pkg_root, "data")
  
  # Create data directory if it doesn't exist
  if (!dir.exists(data_dir)) {
    dir.create(data_dir, recursive = TRUE)
  }
  
  # Load the data files
  location_rda_path <- file.path(data_dir, "location_df.rda")
  provider_rda_path <- file.path(data_dir, "provider_df.rda")
  
  if (!file.exists(location_rda_path)) {
    stop("location_df.rda not found at: ", location_rda_path,
         "\nPlease run build_location_df() first.")
  }
  
  if (!file.exists(provider_rda_path)) {
    stop("provider_df.rda not found at: ", provider_rda_path,
         "\nPlease run build_provider_df() first.")
  }
  
  # Load the datasets
  load(location_rda_path)
  load(provider_rda_path)
  
  # Validate required columns
  if (!"providerId" %in% names(provider_df) || !"providerId" %in% names(location_df)) {
    stop("Both data frames must contain a 'providerId' column.")
  }
  
  # Add prefixes
  names(provider_df) <- paste0("provider_", names(provider_df))
  names(location_df) <- paste0("location_", names(location_df))
  
  # Perform join
  merged_df <- join_fn(
    location_df,
    provider_df,
    by = c("location_providerId" = "provider_providerId")
  )
  
  message("Merged data: ", nrow(merged_df), " records")
  
  # Save merged data
  save_path <- file.path(data_dir, "merged_df.rda")
  save(merged_df, file = save_path, compress = "bzip2")
  message("Merged data saved to: ", save_path)
  
  # Return invisibly
  invisible(merged_df)
}

#-------------------------------------------------------------------------------