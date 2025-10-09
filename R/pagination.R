#' Get paginated data from the CQC API
#'
#' Retrieves data from a paginated endpoint of the CQC API.
#'
#' @param base_url The base URL of the API endpoint. Uses 'Get Location By ID'
#'                 endpoint
#' @param headers A list of HTTP headers for the request.
#' @param query_params A named list of additional query parameters.
#' @param per_page Number of records per page (default is 100).
#' @return A data frame containing combined data from all pages.
#' @export

get_paginated_data <- function(base_url,
                               headers,
                               query_params = list(),
                               per_page = 100) {
  page <- 1
  all_data <- list()

  repeat {
    # Build the query parameters for the current page
    params <- c(query_params,
                list(page = page,
                     perPage = per_page))

    # Make the GET request to the API
    res <- httr::GET(url = base_url,
                 headers,
                 query = params)

    # Stop if there was an HTTP error
    if (httr::http_error(res)) {
      warning("Error fetching page ", page, ": ",
              httr::http_status(res)$message)
      break
    }

    # Parse the JSON response
    content <- jsonlite::fromJSON(httr::content(res,
                                                as = "text",
                                                encoding = "UTF-8"))

    # Extract the actual data payload from known keys
    if ("items" %in% names(content)) {
      data <- content$items
    } else if ("locations" %in% names(content)) {
      data <- content$locations
    } else if ("providers" %in% names(content)) {
      data <- content$providers
    } else {
      data <- content
    }

    # Stop if nothing was returned
    if (length(data) == 0) {
      cat(sprintf("\rPage %d: Retrieved %d records.", page, nrow(data)))
      break
    }

    # Ensure data is a data frame
    if (!is.data.frame(data)) {
      stop(sprintf("Unexpected response format on page %d", page))
    }

    # Inform the user about what was retrieved
    cat(sprintf("\rPage %d: Retrieved %d records.", page, nrow(data)))
    flush.console()

    # Store the page's data
    all_data[[page]] <- data

    # If this page returned fewer records than expected, it was the last
    if (nrow(data) < per_page) {
    #! if (page == 2) { # for debugging
      cat("\nLast page reached.\n")
      break
    }

    page <- page + 1
  }

  # Combine all the data frames into one
  if (length(all_data) > 0) {
    return(dplyr::bind_rows(all_data))
  } else {
    return(data.frame())
  }
}

#-------------------------------------------------------------------------------
