#' Save Provider Data to Stata Format
#'
#' @param data The provider dataframe to save
#' @param filename Output filename (default: "provider_df.dta")
#' @return Invisibly returns TRUE if successful
#' @export
export_prov_dta <- function(data = provider_df, filename = "provider_df.dta") {
  
  # Check if haven is installed
  if (!requireNamespace("haven", quietly = TRUE)) {
    stop("Please install the 'haven' package: install.packages('haven')")
  }
  
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Please install the 'dplyr' package: install.packages('dplyr')")
  }
  
  library(dplyr)
  
  cat("Preparing provider data for Stata export...\n")
  
  # Step 1: Rename long variable names (Stata 32-char limit)
  data_clean <- data %>%
    rename(
      curr_overall_rptDate = currentRatings_overall_reportDate,
      curr_overall_rptLinkId = currentRatings_overall_reportLinkId,
      curr_svc_rptDate = current_serviceRatings_reportDate,
      curr_svc_rptLinkId = current_serviceRatings_reportLinkId,
      curr_kq_Effective = current_keyQuestionRating_Effective,
      curr_kq_Responsive = current_keyQuestionRating_Responsive,
      curr_kq_WellLed = current_keyQuestionRating_Well_led
    )
  
  # Step 2: Shorten any remaining long names
  long_names <- names(data_clean)[nchar(names(data_clean)) > 32]
  if (length(long_names) > 0) {
    cat("Shortening", length(long_names), "additional long variable names\n")
    data_clean <- data_clean %>%
      rename_with(~substr(., 1, 32), .cols = all_of(long_names))
  }
  
  # Step 3: Clean data for Stata compatibility
  cat("Cleaning data for Stata compatibility...\n")
  data_clean <- data_clean %>%
    mutate(across(everything(), ~as.character(.))) %>% # Convert all to character
    mutate(across(everything(), ~ifelse(. == "NA", "", .))) %>% # Replace "NA" strings
    mutate(across(everything(), ~ifelse(is.na(.), "", .))) # Replace actual NAs
  
  # Step 4: Write to Stata format
  cat("Writing to:", filename, "\n")
  haven::write_dta(data_clean, filename)
  
  cat("✓ Successfully saved", nrow(data_clean), "rows and", ncol(data_clean), "columns\n")
  cat("✓ File saved as:", filename, "\n")
  
  invisible(TRUE)
}