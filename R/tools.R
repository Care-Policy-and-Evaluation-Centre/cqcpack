#' Get API Key
#'
#' Prompts the user to enter their API key.
#'
#' @return A character string with the users API key.
#' @export
get_api_key <- function() {

  # Will retrieve the API key from the system variable
  api_key <- Sys.getenv("R_CQC_API_KEY")

  # If the API key is found and is not an empty string, return it immediately.
  if (api_key != "") {
    return(api_key)
  }

  # If no key was set in the environment, prompt the user to input their API key.
  api_key <- readline(prompt = "Please enter your API key: ")

  # If the user enters an empty string, stop with an error message.
  if (!nzchar(api_key)) {
    stop("No API key provided!")
  }
  # Return the API key that the user entered.
  api_key
}

#-------------------------------------------------------------------------------

#' Set HTTP Headers for CQC API
#'
#' Creates the appropriate HTTP headers for authenticating with the CQC API.
#'
#' @param api_key Optional character string containing the CQC API subscription
#'                key. If not provided, it will be retrieved using `get_api_key()`.
#' @return httr headers object that can be used in HTTP requests
#' @export

set_headers <- function(api_key = NULL) {
  # If no API key is provided, call get_api_key() to retrieve it
  if (is.null(api_key)) {
    api_key <- get_api_key()
  }

  # Validate the API key to ensure it's not empty or NA
  if (api_key == "" || is.na(api_key)) {
    stop("API key cannot be empty or NA")
  }

  # Create and return the HTTP headers using the httr package
  headers <- httr::add_headers(
    "Ocp-Apim-Subscription-Key" = api_key,
    "Accept" = "application/json",
    "Content-Type" = "application/json"
  )

  return(headers)
}

#-------------------------------------------------------------------------------
