#' Scrape Providers from the CQC API
#'
#' Retrieves all provider data from the CQC API
#' using the paginated provider endpoint.
#'
#' @param api_key A character string containing your API key.
#' @param per_page Number of records per page.
#' @param verbose Logical; if TRUE, prints number of provider IDs retrieved.
#' @return A data frame of providers data.
#' @export
scrape_providers <- function(api_key,
                             per_page = 100,
                             verbose = FALSE) {

  headers <- set_headers(api_key)
  providers_url <- "https://api.service.cqc.org.uk/public/v1/providers"

  data <- get_paginated_data(
    base_url = providers_url,
    headers = headers,
    per_page = per_page
  )

  # Optional message with number of provider IDs retrieved
  if (verbose && "providerId" %in% names(data)) {
    cat(sprintf("\nRetrieved %d unique provider IDs.\n", length(unique(data$locationId))))
  }

  return(data)
}

#-------------------------------------------------------------------------------
