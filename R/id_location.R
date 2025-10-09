#' Scrape Locations from the CQC API
#'
#' Retrieves all location data from the CQC API
#' using the paginated locations endpoint.
#'
#' @param api_key A character string containing your API key.
#' @param per_page Number of records per page.
#' @param verbose Logical; if TRUE, prints number of location IDs retrieved.
#' @return A data frame of location data.
#' @export
scrape_locations <- function(api_key,
                             per_page = 100,
                             verbose = FALSE) {

  headers <- set_headers(api_key)
  locations_url <- "https://api.service.cqc.org.uk/public/v1/locations"

  data <- get_paginated_data(
    base_url = locations_url,
    headers = headers,
    per_page = per_page
  )

  # Optional message with number of location IDs retrieved
  if (verbose && "locationId" %in% names(data)) {
    cat(sprintf("\rPage %d: Retrieved %d records.", page, nrow(data)))
  }

  return(data)
}

#-------------------------------------------------------------------------------
