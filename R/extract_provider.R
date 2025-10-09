#' Extract Provider Row from JSON File
#'
#' Extracts structured data from a CQC provider JSON file and returns it as a
#' single-row tibble. The function handles various nested structures including
#' contacts, regulated activities, inspection categories, ratings, and reports.
#' Provides options to extract nested data as list columns for detailed analysis.
#'
#' @param file Character. Path to the provider JSON file to process
#' @param json_dir Character. Optional directory path for JSON files (currently unused)
#' @param extract_regulated_activities Logical. If TRUE, extracts regulated activities
#'   as a nested dataframe in the 'regulatedActivities' column. Default: FALSE
#' @param extract_inspection_categories Logical. If TRUE, extracts inspection categories
#'   as a nested dataframe in the 'inspectionCategories' column. Default: FALSE
#' @param extract_key_question_ratings Logical. If TRUE, extracts key question ratings
#'   from currentRatings as a nested dataframe in the 'currentRatings_keyQuestionRatings'
#'   column. Default: FALSE
#' @param extract_reports_nested Logical. If TRUE, extracts all reports as a nested
#'   dataframe in the 'reports' column. Default: FALSE
#' @param extract_contacts_nested Logical. If TRUE, extracts contacts as a nested
#'   dataframe in the 'contacts' column. Default: FALSE
#' @param add_contacts_flag Logical. If TRUE, adds a 'has_contacts_data' boolean
#'   column indicating whether contact information exists. Default: TRUE
#' @param debug_contacts Logical. If TRUE, prints debug information during contacts
#'   extraction. Useful for troubleshooting contact data issues. Default: FALSE
#'
#' @return A tibble with one row containing provider information. Core columns include:
#'   \itemize{
#'     \item \code{providerId} - Unique provider identifier
#'     \item \code{name} - Provider name
#'     \item \code{locationIds} - Semicolon-separated list of associated location IDs
#'     \item \code{organisationType} - Type of organisation
#'     \item \code{registrationStatus} - Current registration status
#'     \item Address fields (\code{postalAddressLine1}, \code{postalCode}, etc.)
#'     \item Rating fields (\code{currentRatings_overall_rating}, key question ratings)
#'     \item Date fields (\code{registrationDate}, \code{lastInspection_date})
#'     \item Optional nested columns based on extraction parameters
#'   }
#'   If an error occurs during processing, returns a minimal tibble with
#'   \code{providerId}, \code{name}, and \code{error_message} columns.
#'
#' @details
#' The function safely handles missing or malformed data by using helper functions
#' that return NA values when data is unavailable. Nested structures are flattened
#' into character fields (semicolon-separated) unless explicitly requested as
#' nested dataframes via the extraction parameters.
#'
#' Key question ratings include: Caring, Effective, Responsive, Safe, and Well-led.
#'
#' The function wraps all processing in a tryCatch block and will return a minimal
#' error tibble if processing fails completely.
#'
#' @examples
#' \dontrun{
#' # Basic extraction with default settings
#' provider_data <- extract_provider_row("provider_1-12345.json")
#'
#' # Extract all nested structures
#' provider_full <- extract_provider_row(
#'   "provider_1-12345.json",
#'   extract_regulated_activities = TRUE,
#'   extract_inspection_categories = TRUE,
#'   extract_key_question_ratings = TRUE,
#'   extract_reports_nested = TRUE,
#'   extract_contacts_nested = TRUE
#' )
#'
#' # Debug contacts extraction
#' provider_debug <- extract_provider_row(
#'   "provider_1-12345.json",
#'   extract_contacts_nested = TRUE,
#'   debug_contacts = TRUE
#' )
#'
#' # Process multiple files
#' json_files <- list.files("provider_jsons/", pattern = "*.json", full.names = TRUE)
#' provider_list <- lapply(json_files, extract_provider_row)
#' provider_df <- dplyr::bind_rows(provider_list)
#' }
#'
#' @seealso
#' \code{\link{extract_location_row}} for similar functionality with location data
#'
#' @export
extract_provider_row <- function(file,
                                 json_dir = NULL,
                                 extract_regulated_activities = FALSE,
                                 extract_inspection_categories = FALSE,
                                 extract_key_question_ratings = FALSE,
                                 extract_reports_nested = FALSE,
                                 extract_contacts_nested = FALSE,
                                 add_contacts_flag = TRUE,
                                 debug_contacts = FALSE) {

  # Wrap the entire function in a tryCatch to handle any remaining edge cases
  tryCatch({

    # Safe null-coalescing helper that handles vectors
    `%||%` <- function(a, b) {
      if (is.null(a) || length(a) == 0) {
        b
      } else {
        # If it's a vector, take the first element
        if (length(a) > 1) {
          a[1]
        } else {
          a
        }
      }
    }

    # Helper function to safely convert to date
    safe_as_date <- function(date_string) {
      if (is.null(date_string) || is.na(date_string) || date_string == "") {
        return(as.Date(NA))
      }
      tryCatch({
        as.Date(date_string)
      }, error = function(e) {
        as.Date(NA)
      })
    }

    # Helper function to ensure single values for tibble construction
    ensure_single_value <- function(value, default = NA_character_) {
      if (is.null(value) || length(value) == 0) {
        return(default)
      } else if (length(value) > 1) {
        return(value[1])
      } else {
        return(value)
      }
    }

    # Helper function to extract fields from arrays
    extract_from_array <- function(array, field_name) {
      tryCatch({
        if (is.null(array) || length(array) == 0 ||
            (is.data.frame(array) && nrow(array) == 0)) {
          return(NA_character_)
        }

        if (!is.list(array)) {
          return(NA_character_)
        }

        values <- vapply(array, function(x) {
          result <- if (is.list(x)) x[[field_name]] %||% "" else ""
          if (length(result) == 0) return("")
          if (length(result) > 1) return(paste(result, collapse = " "))
          return(as.character(result))
        }, character(1))

        non_empty_values <- values[!is.na(values) & !is.null(values) & values != ""]
        if (length(non_empty_values) == 0) {
          return(NA_character_)
        }
        paste(non_empty_values, collapse = "; ")
      }, error = function(e) {
        NA_character_
      })
    }

    # Helper function to safely extract nested fields
    safe_extract <- function(obj, ...) {
      tryCatch({
        path <- list(...)
        result <- obj
        for (key in path) {
          if (!is.list(result) || is.null(result) || is.null(result[[key]])) {
            return(NA_character_)
          }
          result <- result[[key]]
          if (length(result) > 1 && !is.list(result)) {
            result <- result[1]
          }
        }
        if (is.null(result)) {
          return(NA_character_)
        }
        if (length(result) > 1) {
          return(result[1])
        }
        if (is.null(result)) {
          return(NA_character_)
        } else {
          return(as.character(result))
        }
      }, error = function(e) {
        NA_character_
      })
    }

    # Helper function to extract most recent date from potentially multiple date objects
    extract_most_recent_date <- function(obj, date_field) {
      tryCatch({
        if (is.null(obj)) {
          return(NA_character_)
        }

        if (is.list(obj) && !is.null(obj[[date_field]]) && !is.data.frame(obj)) {
          return(obj[[date_field]] %||% NA_character_)
        }

        if (is.data.frame(obj) || (is.list(obj) && length(obj) > 0 && is.list(obj[[1]]))) {
          dates <- if (is.data.frame(obj)) {
            obj[[date_field]]
          } else {
            vapply(obj, function(x) x[[date_field]] %||% "", character(1))
          }

          valid_dates <- dates[!is.na(dates) & !is.null(dates) & dates != ""]
          if (length(valid_dates) == 0) {
            return(NA_character_)
          }
          return(max(valid_dates))
        }

        return(NA_character_)
      }, error = function(e) {
        NA_character_
      })
    }

    # Helper function to extract locationIds array
    extract_location_ids <- function(location_ids) {
      tryCatch({
        if (is.null(location_ids) || length(location_ids) == 0) {
          return(NA_character_)
        }

        if (!is.list(location_ids) && !is.character(location_ids)) {
          return(NA_character_)
        }

        # Convert to character and filter out empty values
        ids <- as.character(location_ids)
        valid_ids <- ids[ids != "" & !is.na(ids) & !is.null(ids)]

        if (length(valid_ids) == 0) {
          return(NA_character_)
        }

        paste(valid_ids, collapse = "; ")
      }, error = function(e) {
        NA_character_
      })
    }

    # Helper function to check if contacts data exists
    contacts_data_exists <- function(contacts) {
      if (is.null(contacts) || length(contacts) == 0) {
        return(FALSE)
      }

      if (is.data.frame(contacts)) {
        return(nrow(contacts) > 0)
      }

      if (is.list(contacts)) {
        # Check if list has elements and they're not all null/empty
        return(length(contacts) > 0 && !all(sapply(contacts, function(x) is.null(x) || length(x) == 0)))
      }

      return(FALSE)
    }

    # Helper function to extract contacts as nested dataframe with better error handling
    extract_contacts_as_nested_df <- function(contacts, debug = FALSE) {
      if (debug) {
        cat("DEBUG: Contacts type:", class(contacts)[1], "\n")
        cat("DEBUG: Contacts length:", if(is.null(contacts)) 0 else length(contacts), "\n")
      }

      # Return empty structure if no contacts
      empty_structure <- list(tibble::tibble(
        personTitle = character(0),
        personGivenName = character(0),
        personFamilyName = character(0),
        personRoles = character(0)
      ))

      if (is.null(contacts) || length(contacts) == 0) {
        if (debug) cat("DEBUG: No contacts data found\n")
        return(empty_structure)
      }

      # Handle data.frame structure
      if (is.data.frame(contacts)) {
        if (nrow(contacts) == 0) {
          if (debug) cat("DEBUG: Contacts dataframe is empty\n")
          return(empty_structure)
        }

        if (debug) {
          cat("DEBUG: Processing contacts dataframe with", nrow(contacts), "rows\n")
          cat("DEBUG: Columns available:", paste(colnames(contacts), collapse = ", "), "\n")
        }

        # Create result dataframe with available columns
        result_df <- tibble::tibble(
          personTitle = if ("personTitle" %in% colnames(contacts)) {
            contacts$personTitle
          } else {
            rep(NA_character_, nrow(contacts))
          },
          personGivenName = if ("personGivenName" %in% colnames(contacts)) {
            contacts$personGivenName
          } else {
            rep(NA_character_, nrow(contacts))
          },
          personFamilyName = if ("personFamilyName" %in% colnames(contacts)) {
            contacts$personFamilyName
          } else {
            rep(NA_character_, nrow(contacts))
          },
          personRoles = if ("personRoles" %in% colnames(contacts)) {
            # Handle List column - convert each element to comma-separated string
            sapply(contacts$personRoles, function(roles) {
              if (!is.null(roles) && length(roles) > 0) {
                # Handle both character vectors and lists
                if (is.list(roles)) {
                  paste(unlist(roles), collapse = ", ")
                } else {
                  paste(roles, collapse = ", ")
                }
              } else {
                NA_character_
              }
            })
          } else {
            rep(NA_character_, nrow(contacts))
          }
        )

        if (debug) {
          cat("DEBUG: Successfully extracted", nrow(result_df), "contacts\n")
        }

        return(list(result_df))
      }

      # Handle list structure (fallback)
      if (is.list(contacts)) {
        if (debug) cat("DEBUG: Processing contacts list with", length(contacts), "elements\n")

        contact_rows <- lapply(seq_along(contacts), function(i) {
          contact <- contacts[[i]]

          if (!is.list(contact)) {
            if (debug) cat("DEBUG: Contact", i, "is not a list, skipping\n")
            return(tibble::tibble(
              personTitle = NA_character_,
              personGivenName = NA_character_,
              personFamilyName = NA_character_,
              personRoles = NA_character_
            ))
          }

          # Extract roles with better handling
          roles_str <- if (!is.null(contact$personRoles)) {
            if (is.list(contact$personRoles) || is.character(contact$personRoles)) {
              paste(unlist(contact$personRoles), collapse = ", ")
            } else {
              as.character(contact$personRoles)
            }
          } else {
            NA_character_
          }

          tibble::tibble(
            personTitle = contact$personTitle %||% NA_character_,
            personGivenName = contact$personGivenName %||% NA_character_,
            personFamilyName = contact$personFamilyName %||% NA_character_,
            personRoles = roles_str
          )
        })

        result_df <- do.call(rbind, contact_rows)

        if (debug) {
          cat("DEBUG: Successfully processed", nrow(result_df), "contacts from list\n")
        }

        return(list(result_df))
      }

      if (debug) cat("DEBUG: Unhandled contacts structure, returning empty\n")
      return(empty_structure)
    }

    # Helper function to extract provider contacts (only used when not using nested)
    extract_provider_contact_field <- function(contacts, field_name) {
      tryCatch({
        if (is.null(contacts) || length(contacts) == 0) {
          return(NA_character_)
        }

        # Handle data.frame structure (which is what we actually have)
        if (is.data.frame(contacts)) {
          if (nrow(contacts) == 0 || !field_name %in% colnames(contacts)) {
            return(NA_character_)
          }

          if (field_name == "personRoles") {
            # personRoles is a List column - each element is a character vector
            roles_list <- contacts[[field_name]]
            all_roles <- c()
            for (i in seq_along(roles_list)) {
              if (!is.null(roles_list[[i]]) && length(roles_list[[i]]) > 0) {
                all_roles <- c(all_roles, paste(roles_list[[i]], collapse = ", "))
              }
            }
            if (length(all_roles) > 0) {
              return(paste(all_roles, collapse = "; "))
            } else {
              return(NA_character_)
            }
          } else {
            # For other fields, just extract the column values
            field_values <- contacts[[field_name]]
            valid_values <- field_values[!is.na(field_values) & field_values != ""]
            if (length(valid_values) > 0) {
              return(paste(valid_values, collapse = "; "))
            } else {
              return(NA_character_)
            }
          }
        }

        # Handle list structure (fallback for other possible structures)
        if (is.list(contacts)) {
          all_values <- c()

          for (contact in contacts) {
            if (!is.list(contact)) next

            value <- contact[[field_name]]
            if (!is.null(value)) {
              if (field_name == "personRoles" && is.list(value)) {
                # Handle roles as an array
                all_values <- c(all_values, paste(value, collapse = ", "))
              } else {
                all_values <- c(all_values, as.character(value))
              }
            }
          }

          if (length(all_values) == 0) {
            return(NA_character_)
          }

          return(paste(all_values, collapse = "; "))
        }

        return(NA_character_)
      }, error = function(e) {
        NA_character_
      })
    }

    # Helper function to extract regulated activities as nested dataframe
    extract_regulated_activities_as_nested_df <- function(reg_activities) {
      if (is.null(reg_activities) || length(reg_activities) == 0) {
        return(list(tibble::tibble(
          regulatedActivities_name = character(0),
          regulatedActivities_code = character(0),
          regulatedActivities_nominatedIndividual_personTitle = character(0),
          regulatedActivities_nominatedIndividual_personGivenName = character(0),
          regulatedActivities_nominatedIndividual_personFamilyName = character(0)
        )))
      }

      if (is.data.frame(reg_activities)) {
        result_df <- tibble::tibble(
          regulatedActivities_name = if ("name" %in% colnames(reg_activities)) reg_activities$name else NA_character_,
          regulatedActivities_code = if ("code" %in% colnames(reg_activities)) reg_activities$code else NA_character_,
          regulatedActivities_nominatedIndividual_personTitle = if ("nominatedIndividual" %in% colnames(reg_activities)) {
            vapply(reg_activities$nominatedIndividual, function(ni) {
              if (is.list(ni)) ni$personTitle %||% NA_character_ else NA_character_
            }, character(1))
          } else {
            NA_character_
          },
          regulatedActivities_nominatedIndividual_personGivenName = if ("nominatedIndividual" %in% colnames(reg_activities)) {
            vapply(reg_activities$nominatedIndividual, function(ni) {
              if (is.list(ni)) ni$personGivenName %||% NA_character_ else NA_character_
            }, character(1))
          } else {
            NA_character_
          },
          regulatedActivities_nominatedIndividual_personFamilyName = if ("nominatedIndividual" %in% colnames(reg_activities)) {
            vapply(reg_activities$nominatedIndividual, function(ni) {
              if (is.list(ni)) ni$personFamilyName %||% NA_character_ else NA_character_
            }, character(1))
          } else {
            NA_character_
          }
        )
        return(list(result_df))
      }

      if (!is.list(reg_activities)) {
        return(list(tibble::tibble(
          regulatedActivities_name = character(0),
          regulatedActivities_code = character(0),
          regulatedActivities_nominatedIndividual_personTitle = character(0),
          regulatedActivities_nominatedIndividual_personGivenName = character(0),
          regulatedActivities_nominatedIndividual_personFamilyName = character(0)
        )))
      }

      # Extract each regulated activity into a row
      activity_rows <- lapply(reg_activities, function(activity) {
        if (!is.list(activity)) {
          return(tibble::tibble(
            regulatedActivities_name = NA_character_,
            regulatedActivities_code = NA_character_,
            regulatedActivities_nominatedIndividual_personTitle = NA_character_,
            regulatedActivities_nominatedIndividual_personGivenName = NA_character_,
            regulatedActivities_nominatedIndividual_personFamilyName = NA_character_
          ))
        }

        # Extract nominated individual details
        nom_individual <- activity$nominatedIndividual

        tibble::tibble(
          regulatedActivities_name = activity$name %||% NA_character_,
          regulatedActivities_code = activity$code %||% NA_character_,
          regulatedActivities_nominatedIndividual_personTitle = if (is.list(nom_individual)) nom_individual$personTitle %||% NA_character_ else NA_character_,
          regulatedActivities_nominatedIndividual_personGivenName = if (is.list(nom_individual)) nom_individual$personGivenName %||% NA_character_ else NA_character_,
          regulatedActivities_nominatedIndividual_personFamilyName = if (is.list(nom_individual)) nom_individual$personFamilyName %||% NA_character_ else NA_character_
        )
      })

      # Combine all rows into one dataframe
      result_df <- do.call(rbind, activity_rows)
      list(result_df)
    }

    # Helper function to extract inspectionCategories as nested dataframe
    extract_inspection_categories_as_nested_df <- function(categories) {
      if (is.null(categories) || length(categories) == 0) {
        return(list(tibble::tibble(primary = character(0), code = character(0), name = character(0))))
      }

      if (is.data.frame(categories)) {
        result_df <- tibble::tibble(
          primary = if ("primary" %in% colnames(categories)) categories$primary else NA_character_,
          code = if ("code" %in% colnames(categories)) categories$code else NA_character_,
          name = if ("name" %in% colnames(categories)) categories$name else NA_character_
        )
        return(list(result_df))
      }

      if (!is.list(categories)) {
        return(list(tibble::tibble(primary = character(0), code = character(0), name = character(0))))
      }

      # Extract each category into a row
      category_rows <- lapply(categories, function(cat) {
        if (!is.list(cat)) {
          return(tibble::tibble(primary = NA_character_, code = NA_character_, name = NA_character_))
        }

        tibble::tibble(
          primary = cat$primary %||% NA_character_,
          code = cat$code %||% NA_character_,
          name = cat$name %||% NA_character_
        )
      })

      # Combine all rows into one dataframe
      result_df <- do.call(rbind, category_rows)
      list(result_df)
    }

    # Helper function to extract keyQuestionRatings as nested dataframe
    extract_key_question_ratings_as_nested_df <- function(ratings_obj) {
      if (is.null(ratings_obj) || !is.list(ratings_obj)) {
        return(list(tibble::tibble(name = character(0), rating = character(0), reportDate = character(0), reportLinkId = character(0))))
      }

      key_ratings <- ratings_obj$keyQuestionRatings

      if (is.null(key_ratings) || length(key_ratings) == 0) {
        return(list(tibble::tibble(name = character(0), rating = character(0), reportDate = character(0), reportLinkId = character(0))))
      }

      if (is.data.frame(key_ratings)) {
        result_df <- tibble::tibble(
          name = if ("name" %in% colnames(key_ratings)) key_ratings$name else NA_character_,
          rating = if ("rating" %in% colnames(key_ratings)) key_ratings$rating else NA_character_,
          reportDate = if ("reportDate" %in% colnames(key_ratings)) key_ratings$reportDate else NA_character_,
          reportLinkId = if ("reportLinkId" %in% colnames(key_ratings)) key_ratings$reportLinkId else NA_character_
        )
        return(list(result_df))
      }

      if (!is.list(key_ratings)) {
        return(list(tibble::tibble(name = character(0), rating = character(0), reportDate = character(0), reportLinkId = character(0))))
      }

      # Extract each key question rating into a row
      rating_rows <- lapply(key_ratings, function(kq) {
        if (!is.list(kq)) {
          return(tibble::tibble(name = NA_character_, rating = NA_character_, reportDate = NA_character_, reportLinkId = NA_character_))
        }

        tibble::tibble(
          name = kq$name %||% NA_character_,
          rating = kq$rating %||% NA_character_,
          reportDate = kq$reportDate %||% NA_character_,
          reportLinkId = kq$reportLinkId %||% NA_character_
        )
      })

      # Combine all rows into one dataframe
      result_df <- do.call(rbind, rating_rows)
      list(result_df)
    }

    # Helper function to extract all reports as nested dataframe
    extract_reports_as_nested_df <- function(reports) {
      if (is.null(reports) || length(reports) == 0) {
        return(list(tibble::tibble(
          linkId = character(0),
          reportDate = as.Date(character(0)),
          reportUri = character(0),
          reportType = character(0)
        )))
      }

      if (is.data.frame(reports)) {
        result_df <- tibble::tibble(
          linkId = if ("linkId" %in% colnames(reports)) reports$linkId else NA_character_,
          reportDate = if ("reportDate" %in% colnames(reports)) as.Date(reports$reportDate) else as.Date(NA),
          reportUri = if ("reportUri" %in% colnames(reports)) reports$reportUri else NA_character_,
          reportType = if ("reportType" %in% colnames(reports)) reports$reportType else NA_character_
        )
        return(list(result_df))
      }

      if (!is.list(reports)) {
        return(list(tibble::tibble(
          linkId = character(0),
          reportDate = as.Date(character(0)),
          reportUri = character(0),
          reportType = character(0)
        )))
      }

      # Extract each report into a row
      report_rows <- lapply(reports, function(report) {
        if (!is.list(report)) {
          return(tibble::tibble(
            linkId = NA_character_,
            reportDate = as.Date(NA),
            reportUri = NA_character_,
            reportType = NA_character_
          ))
        }

        tibble::tibble(
          linkId = report$linkId %||% NA_character_,
          reportDate = if (!is.null(report$reportDate)) as.Date(report$reportDate) else as.Date(NA),
          reportUri = report$reportUri %||% NA_character_,
          reportType = report$reportType %||% NA_character_
        )
      })

      # Combine all rows into one dataframe
      result_df <- do.call(rbind, report_rows)
      list(result_df)
    }

    # Helper function to extract individual key question ratings from currentRatings serviceRatings
    extract_service_key_question_rating <- function(service_ratings, question_name) {
      tryCatch({
        if (is.null(service_ratings) || length(service_ratings) == 0) {
          return(NA_character_)
        }

        # If it's a data frame, take the first row
        if (is.data.frame(service_ratings)) {
          if (nrow(service_ratings) == 0 || !"keyQuestionRatings" %in% colnames(service_ratings)) {
            return(NA_character_)
          }

          # Get keyQuestionRatings from first service
          kq_ratings <- service_ratings$keyQuestionRatings[[1]]

          if (is.null(kq_ratings) || length(kq_ratings) == 0) {
            return(NA_character_)
          }

          if (is.data.frame(kq_ratings) && "name" %in% colnames(kq_ratings) && "rating" %in% colnames(kq_ratings)) {
            matching_row <- which(kq_ratings$name == question_name)
            if (length(matching_row) > 0) {
              return(as.character(kq_ratings$rating[matching_row[1]]))
            }
          }
        } else if (is.list(service_ratings)) {
          # Take first service from list
          if (length(service_ratings) == 0 || !is.list(service_ratings[[1]])) {
            return(NA_character_)
          }

          first_service <- service_ratings[[1]]
          kq_ratings <- first_service$keyQuestionRatings

          if (is.null(kq_ratings) || length(kq_ratings) == 0) {
            return(NA_character_)
          }

          if (is.data.frame(kq_ratings) && "name" %in% colnames(kq_ratings) && "rating" %in% colnames(kq_ratings)) {
            matching_row <- which(kq_ratings$name == question_name)
            if (length(matching_row) > 0) {
              return(as.character(kq_ratings$rating[matching_row[1]]))
            }
          } else if (is.list(kq_ratings)) {
            for (kq in kq_ratings) {
              if (is.list(kq) && !is.null(kq$name) && kq$name == question_name) {
                return(as.character(kq$rating %||% NA_character_))
              }
            }
          }
        }

        return(NA_character_)
      }, error = function(e) {
        NA_character_
      })
    }

    # Helper function to extract first service rating field
    extract_first_service_rating_field <- function(service_ratings, field_name) {
      tryCatch({
        if (is.null(service_ratings) || length(service_ratings) == 0) {
          return(NA_character_)
        }

        if (is.data.frame(service_ratings)) {
          if (nrow(service_ratings) == 0 || !field_name %in% colnames(service_ratings)) {
            return(NA_character_)
          }
          return(as.character(service_ratings[[field_name]][1]))
        } else if (is.list(service_ratings)) {
          if (length(service_ratings) == 0 || !is.list(service_ratings[[1]])) {
            return(NA_character_)
          }
          first_service <- service_ratings[[1]]
          return(as.character(first_service[[field_name]] %||% NA_character_))
        }

        return(NA_character_)
      }, error = function(e) {
        NA_character_
      })
    }

    # Read JSON
    x <- jsonlite::fromJSON(file, simplifyVector = TRUE)

    # NEW: Check if contacts data exists (for flag)
    has_contacts <- contacts_data_exists(x$contacts)

    # Extract conditional nested columns
    regulated_activities_column <- if (extract_regulated_activities) {
      list(regulatedActivities = extract_regulated_activities_as_nested_df(x$regulatedActivities))
    } else {
      list()
    }

    inspection_categories_column <- if (extract_inspection_categories) {
      list(inspectionCategories = extract_inspection_categories_as_nested_df(x$inspectionCategories))
    } else {
      list()
    }

    key_question_ratings_column <- if (extract_key_question_ratings) {
      list(currentRatings_keyQuestionRatings = extract_key_question_ratings_as_nested_df(x$currentRatings$overall))
    } else {
      list()
    }

    reports_nested_column <- if (extract_reports_nested) {
      list(reports = extract_reports_as_nested_df(x$reports))
    } else {
      list()
    }

    # Use the enhanced contacts extraction with debug option
    contacts_nested_column <- if (extract_contacts_nested) {
      list(contacts = extract_contacts_as_nested_df(x$contacts, debug = debug_contacts))
    } else {
      list()
    }

    # Add contacts flag column (optional)
    contacts_flag_column <- if (add_contacts_flag) {
      list(has_contacts_data = has_contacts)
    } else {
      list()
    }

    # Build main tibble
    tibble::tibble(
      # Basic fields (ensure single values)
      providerId = ensure_single_value(x$providerId %||% NA_character_),
      locationIds = extract_location_ids(x$locationIds),
      organisationType = ensure_single_value(x$organisationType %||% NA_character_),
      ownershipType = ensure_single_value(x$ownershipType %||% NA_character_),
      type = ensure_single_value(x$type %||% NA_character_),
      name = ensure_single_value(x$name %||% NA_character_),
      brandId = ensure_single_value(x$brandId %||% NA_character_),
      brandName = ensure_single_value(x$brandName %||% NA_character_),
      odsCode = ensure_single_value(x$odsCode %||% NA_character_),
      registrationStatus = ensure_single_value(x$registrationStatus %||% NA_character_),
      registrationDate = ensure_single_value(x$registrationDate %||% NA_character_),
      companiesHouseNumber = ensure_single_value(x$companiesHouseNumber %||% NA_character_),
      charityNumber = ensure_single_value(x$charityNumber %||% NA_character_),
      website = ensure_single_value(x$website %||% NA_character_),
      deregistrationDate = ensure_single_value(x$deregistrationDate %||% NA_character_),
      uprn = ensure_single_value(x$uprn %||% NA_character_),
      onspdLatitude = x$onspdLatitude %||% NA_real_,
      onspdLongitude = x$onspdLongitude %||% NA_real_,
      onspdIcbCode = x$onspdIcbCode %||% NA_character_,
      onspdIcbName = x$onspdIcbName %||% NA_character_,
      mainPhoneNumber = ensure_single_value(x$mainPhoneNumber %||% NA_character_),
      inspectionDirectorate = ensure_single_value(x$inspectionDirectorate %||% NA_character_),
      constituency = x$constituency %||% NA_character_,
      localAuthority = x$localAuthority %||% NA_character_,

      # Address fields
      postalAddressLine1 = x$postalAddressLine1 %||% NA_character_,
      postalAddressLine2 = x$postalAddressLine2 %||% NA_character_,
      postalAddressTownCity = x$postalAddressTownCity %||% NA_character_,
      postalAddressCounty = x$postalAddressCounty %||% NA_character_,
      region = x$region %||% NA_character_,
      postalCode = x$postalCode %||% NA_character_,

      # Nested date fields (extract most recent if multiple)
      lastInspection_date = extract_most_recent_date(x$lastInspection, "date"),
      lastReport_publicationDate = extract_most_recent_date(x$lastReport, "publicationDate"),

      # NEW: ADD CONTACTS FLAG COLUMN (conditional)
      !!!contacts_flag_column,

      # ADD CONTACTS NESTED COLUMN (conditional) -- now with improved extraction
      !!!contacts_nested_column,

      # ADD REGULATED ACTIVITIES COLUMN (conditional)
      !!!regulated_activities_column,

      # ADD INSPECTION CATEGORIES COLUMN (conditional)
      !!!inspection_categories_column,

      # Current ratings - detailed overall ratings
      currentRatings_overall_rating = safe_extract(x, "currentRatings", "overall", "rating"),
      currentRatings_overall_reportDate = safe_extract(x, "currentRatings", "overall", "reportDate"),
      currentRatings_overall_reportLinkId = safe_extract(x, "currentRatings", "overall", "reportLinkId"),

      # Current ratings - provider-level currentRatings (NEW SECTION)
      currentRatings_reportDate = safe_extract(x, "currentRatings", "reportDate"),
      current_serviceRatings_name = extract_first_service_rating_field(x$currentRatings$serviceRatings, "name"),
      current_serviceRatings_rating = extract_first_service_rating_field(x$currentRatings$serviceRatings, "rating"),
      current_serviceRatings_reportDate = extract_first_service_rating_field(x$currentRatings$serviceRatings, "reportDate"),
      current_serviceRatings_reportLinkId = extract_first_service_rating_field(x$currentRatings$serviceRatings, "reportLinkId"),
      current_keyQuestionRating_Caring = extract_service_key_question_rating(x$currentRatings$serviceRatings, "Caring"),
      current_keyQuestionRating_Effective = extract_service_key_question_rating(x$currentRatings$serviceRatings, "Effective"),
      current_keyQuestionRating_Responsive = extract_service_key_question_rating(x$currentRatings$serviceRatings, "Responsive"),
      current_keyQuestionRating_Safe = extract_service_key_question_rating(x$currentRatings$serviceRatings, "Safe"),
      current_keyQuestionRating_Well_led = extract_service_key_question_rating(x$currentRatings$serviceRatings, "Well-led"),

      # ADD KEY QUESTION RATINGS COLUMN (conditional)
      !!!key_question_ratings_column,

      # ADD REPORTS NESTED COLUMN (conditional)
      !!!reports_nested_column

      # ADD UNPUBLISHED REPORTS NESTED COLUMN (conditional) - NOT APPLICABLE TO PROVIDER FILES
      # Note: unpublishedReports does not exist in provider JSON files based on search
    )

  }, error = function(e) {
    # If any error occurs, return a minimal tibble with NA values
    warning("Error in extract_provider_row for file: ", file, " - ", e$message)

    # Create minimal tibble with basic structure
    tibble::tibble(
      providerId = NA_character_,
      name = NA_character_,
      error_message = e$message,
      .rows = 1
    )
  })
}
