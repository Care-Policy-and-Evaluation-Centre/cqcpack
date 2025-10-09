#' Merge Provider and Location Data and Save to Package
#'
#' @param join_fn Join function to use (default: dplyr::inner_join)
#' @return Invisibly returns the merged data frame
#' @export
merge_provider_location <- function(join_fn = dplyr::inner_join) {
  
  # This is exactly what View() does - use standard R name resolution
  if (!exists("location_df")) {
    stop("location_df not found in search path")
  }
  if (!exists("provider_df")) {
    stop("provider_df not found in search path")
  }
  
  provider_df = provider_df
  location_df = location_df
  
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
  data_folder <- "data"
  if (!dir.exists(data_folder)) {
    dir.create(data_folder, recursive = TRUE)
  }
  
  save_path <- file.path(data_folder, "merged_df.rda")
  save(merged_df, file = save_path, compress = "bzip2")
  cat("Merged data saved to:", save_path, "\n")
  
  # Return invisibly (moved to the end)
  invisible(merged_df)
}