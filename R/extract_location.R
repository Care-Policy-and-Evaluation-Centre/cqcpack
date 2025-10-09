#' Extract Location Row from JSON File
#'
#' Processes a single CQC location JSON file and converts it into a one-row tibble
#' with standardized data structure for analysis and merging.
#'
#' @param file Path to a single CQC location JSON file.
#' @param json_dir Path to directory containing JSON files (optional, not used).
#' @param all_specialisms Vector of specialisms to create boolean columns for. Default uses standard 12 specialisms.
#' @param extract_regulated_activities Logical. Include regulated activities as nested dataframes. Default: FALSE.
#' @param extract_reports_nested Logical. Include all reports as nested dataframe. Default: FALSE.
#' @param extract_unpublished_reports_nested Logical. Include unpublished reports as nested dataframe. Default: FALSE.
#'
#' @return A tibble with one row containing location data including basic information, 
#'   address details, boolean specialism and service type columns, current ratings, 
#'   and most recent report information.
#'
#' @export
extract_location_row <- function(file, json_dir = NULL, all_specialisms = NULL, extract_regulated_activities = FALSE, extract_reports_nested = FALSE, extract_unpublished_reports_nested = FALSE) {

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

    # Use default specialisms list if not provided
    if (is.null(all_specialisms)) {
      all_specialisms <- c(
        "caring_for_adults_over_65_yrs", "dementia", "physical_disabilities",
        "sensory_impairment", "caring_for_adults_under_65_yrs", "services_for_everyone",
        "learning_disabilities", "mental_health_conditions",
        "caring_for_people_whose_rights_are_restricted_under_the_mental_health_act",
        "eating_disorders", "substance_misuse_problems", "caring_for_children"
      )
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

        if (is.character(array)) {
          if (field_name == "type") {
            valid_values <- array[!is.na(array) & !is.null(array) & array != ""]
            if (length(valid_values) == 0) {
              return(NA_character_)
            }
            return(paste(valid_values, collapse = "; "))
          } else {
            return(NA_character_)
          }
        }

        if (is.data.frame(array)) {
          if (field_name %in% colnames(array)) {
            values <- array[[field_name]]
            valid_values <- values[!is.na(values) & !is.null(values) & values != ""]
            if (length(valid_values) == 0) {
              return(NA_character_)
            }
            return(paste(valid_values, collapse = "; "))
          } else {
            return(NA_character_)
          }
        }

        if (!is.list(array)) {
          return(NA_character_)
        }

        if (field_name == "type") {
          has_type_field <- tryCatch({
            all(sapply(array, function(x) is.list(x) && "type" %in% names(x)))
          }, error = function(e) FALSE)

          if (has_type_field) {
            types <- sapply(array, function(x) x$type)
            transformed_types <- chartr(" ", "_", tolower(types))
            valid_types <- transformed_types[!is.na(transformed_types) & !is.null(transformed_types) & transformed_types != ""]
            if (length(valid_types) == 0) {
              return(NA_character_)
            }
            return(paste(valid_types, collapse = "; "))
          }
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

    # Helper function to extract most recent report
    extract_most_recent_report <- function(reports) {
      tryCatch({
        if (is.null(reports) || length(reports) == 0) {
          return(list(
            linkId = NA_character_,
            reportDate = as.Date(NA),
            reportUri = NA_character_,
            reportType = NA_character_
          ))
        }

        if (is.data.frame(reports)) {
          if (nrow(reports) == 0) {
            return(list(
              linkId = NA_character_,
              reportDate = as.Date(NA),
              reportUri = NA_character_,
              reportType = NA_character_
            ))
          }

          if ("reportDate" %in% colnames(reports)) {
            report_dates <- as.Date(reports$reportDate)
            most_recent_idx <- which.max(report_dates)

            return(list(
              linkId = if ("linkId" %in% colnames(reports)) reports$linkId[most_recent_idx] else NA_character_,
              reportDate = report_dates[most_recent_idx],
              reportUri = if ("reportUri" %in% colnames(reports)) reports$reportUri[most_recent_idx] else NA_character_,
              reportType = if ("reportType" %in% colnames(reports)) reports$reportType[most_recent_idx] else NA_character_
            ))
          }
        }

        if (is.list(reports)) {
          if (length(reports) == 0) {
            return(list(
              linkId = NA_character_,
              reportDate = as.Date(NA),
              reportUri = NA_character_,
              reportType = NA_character_
            ))
          }

          most_recent_report <- NULL
          most_recent_date <- as.Date("1900-01-01")

          for (report in reports) {
            if (is.list(report) && !is.null(report$reportDate)) {
              report_date <- as.Date(report$reportDate)
              if (!is.na(report_date) && report_date > most_recent_date) {
                most_recent_date <- report_date
                most_recent_report <- report
              }
            }
          }

          if (!is.null(most_recent_report)) {
            return(list(
              linkId = most_recent_report$linkId %||% NA_character_,
              reportDate = most_recent_date,
              reportUri = most_recent_report$reportUri %||% NA_character_,
              reportType = most_recent_report$reportType %||% NA_character_
            ))
          }
        }

        return(list(
          linkId = NA_character_,
          reportDate = as.Date(NA),
          reportUri = NA_character_,
          reportType = NA_character_
        ))
      }, error = function(e) {
        list(
          linkId = NA_character_,
          reportDate = as.Date(NA),
          reportUri = NA_character_,
          reportType = NA_character_
        )
      })
    }

    # Helper function to extract individual key question ratings
    extract_individual_key_question_rating <- function(ratings_obj, question_name) {
      tryCatch({
        if (is.null(ratings_obj) || !is.list(ratings_obj)) {
          return(NA_character_)
        }

        key_ratings <- ratings_obj$keyQuestionRatings

        if (is.null(key_ratings) || length(key_ratings) == 0) {
          return(NA_character_)
        }

        if (is.data.frame(key_ratings)) {
          if ("name" %in% colnames(key_ratings) && "rating" %in% colnames(key_ratings)) {
            name_matches <- key_ratings$name == question_name
            if (length(name_matches) > 0 && any(name_matches, na.rm = TRUE)) {
              matching_indices <- which(name_matches)
              if (length(matching_indices) > 0) {
                rating_value <- key_ratings$rating[matching_indices[1]]
                if (length(rating_value) > 1) rating_value <- rating_value[1]
                return(as.character(rating_value))
              }
            }
          }
          return(NA_character_)
        }

        if (is.list(key_ratings)) {
          for (rating in key_ratings) {
            if (is.list(rating) && !is.null(rating$name)) {
              if (length(rating$name) > 0 && rating$name[1] == question_name) {
                rating_value <- rating$rating %||% NA_character_
                if (length(rating_value) > 1) rating_value <- rating_value[1]
                return(as.character(rating_value))
              }
            }
          }
        }

        return(NA_character_)
      }, error = function(e) {
        NA_character_
      })
    }

    # Helper function to create boolean columns for gacServiceTypes
    extract_gac_service_types_boolean <- function(gac_service_data) {
      tryCatch({
        all_gac_service_types <- c(
          "ambulances", "blood_and_transplant_service", "clinic", "community_health_service",
          "community_services_healthcare", "community_services_learning_disabilities",
          "community_services_mental_health", "community_services_nursing",
          "community_services_substance_abuse", "dentist", "diagnosis_screening",
          "doctors_gps", "home_hospice_care", "homecare_agencies", "hospice", "hospital",
          "hospitals_mental_health_capacity", "hyperbaric_chamber_services",
          "long_term_conditions", "mobile_doctors", "nursing_homes", "phone_online_advice",
          "prison_healthcare", "rehabilitation_illness_injury", "rehabilitation_substance_abuse",
          "residential_homes", "shared_lives", "specialist_college_service", "supported_housing",
          "supported_living", "urgent_care_centres"
        )

        column_names <- sapply(all_gac_service_types, function(service) {
          return(paste0("gacServiceTypes_", service))
        })

        current_services <- c()

        if (!is.null(gac_service_data) && length(gac_service_data) > 0) {
          if (is.data.frame(gac_service_data)) {
            if ("name" %in% colnames(gac_service_data)) {
              current_services <- gac_service_data$name
            }
          } else if (is.list(gac_service_data)) {
            current_services <- sapply(gac_service_data, function(x) {
              if (is.list(x) && "name" %in% names(x)) {
                return(x$name)
              }
              return(NA_character_)
            })
            current_services <- current_services[!is.na(current_services)]
          }

          if (length(current_services) > 0) {
            current_services <- tolower(current_services)
            current_services <- gsub("[^a-z0-9]+", "_", current_services)
            current_services <- gsub("^_+|_+$", "", current_services)
            current_services <- gsub("_+", "_", current_services)
            current_services <- current_services[!is.na(current_services) & !is.null(current_services) & current_services != ""]
          }
        }

        boolean_values <- sapply(all_gac_service_types, function(service_type) {
          tryCatch({
            clean_services <- current_services[!is.na(current_services) & !is.null(current_services)]
            if (length(clean_services) == 0) {
              return(FALSE)
            }
            matches <- service_type == clean_services
            return(any(matches, na.rm = TRUE))
          }, error = function(e) {
            FALSE
          })
        })

        result <- setNames(as.list(boolean_values), column_names)
        return(result)
      }, error = function(e) {
        all_gac_service_types <- c(
          "ambulances", "blood_and_transplant_service", "clinic", "community_health_service",
          "community_services_healthcare", "community_services_learning_disabilities",
          "community_services_mental_health", "community_services_nursing",
          "community_services_substance_abuse", "dentist", "diagnosis_screening",
          "doctors_gps", "home_hospice_care", "homecare_agencies", "hospice", "hospital",
          "hospitals_mental_health_capacity", "hyperbaric_chamber_services",
          "long_term_conditions", "mobile_doctors", "nursing_homes", "phone_online_advice",
          "prison_healthcare", "rehabilitation_illness_injury", "rehabilitation_substance_abuse",
          "residential_homes", "shared_lives", "specialist_college_service", "supported_housing",
          "supported_living", "urgent_care_centres"
        )
        column_names <- sapply(all_gac_service_types, function(service) paste0("gacServiceTypes_", service))
        result <- setNames(as.list(rep(FALSE, length(all_gac_service_types))), column_names)
        return(result)
      })
    }

    # Helper function to create boolean columns for specialisms
    extract_specialisms_boolean <- function(specialisms_data, all_specialisms) {
      tryCatch({
        rename_mapping <- list(
          "caring_for_people_whose_rights_are_restricted_under_the_mental_health_act" = "restricted_mha_care"
        )

        column_names <- sapply(all_specialisms, function(spec) {
          if (spec %in% names(rename_mapping)) {
            return(paste0("specialism_", rename_mapping[[spec]]))
          } else {
            return(paste0("specialism_", spec))
          }
        })

        current_specialisms <- c()

        if (!is.null(specialisms_data) && length(specialisms_data) > 0) {
          if (is.data.frame(specialisms_data)) {
            if ("name" %in% colnames(specialisms_data)) {
              current_specialisms <- specialisms_data$name
            }
          } else if (is.list(specialisms_data)) {
            current_specialisms <- sapply(specialisms_data, function(x) {
              if (is.list(x) && "name" %in% names(x)) {
                return(x$name)
              }
              return(NA_character_)
            })
            current_specialisms <- current_specialisms[!is.na(current_specialisms)]
          }

          if (length(current_specialisms) > 0) {
            current_specialisms <- tolower(current_specialisms)
            current_specialisms <- chartr(" ", "_", current_specialisms)
            current_specialisms <- current_specialisms[!is.na(current_specialisms) & !is.null(current_specialisms) & current_specialisms != ""]
          }
        }

        boolean_values <- sapply(all_specialisms, function(specialism) {
          tryCatch({
            clean_specialisms <- current_specialisms[!is.na(current_specialisms) & !is.null(current_specialisms)]
            if (length(clean_specialisms) == 0) {
              return(FALSE)
            }
            matches <- specialism == clean_specialisms
            return(any(matches, na.rm = TRUE))
          }, error = function(e) {
            FALSE
          })
        })

        result <- setNames(as.list(boolean_values), column_names)
        return(result)
      }, error = function(e) {
        column_names <- sapply(all_specialisms, function(spec) paste0("specialism_", spec))
        result <- setNames(as.list(rep(FALSE, length(all_specialisms))), column_names)
        return(result)
      })
    }

    # Helper function to extract all reports as nested dataframe
    extract_reports_as_nested_df <- function(reports) {
      if (is.null(reports) || length(reports) == 0) {
        return(list(tibble::tibble(
          linkId = character(0),
          reportDate = as.Date(character(0)),
          firstVisitDate = as.Date(character(0)),
          reportUri = character(0),
          reportType = character(0)
        )))
      }

      if (is.data.frame(reports)) {
        result_df <- tibble::tibble(
          linkId = if ("linkId" %in% colnames(reports)) reports$linkId else NA_character_,
          reportDate = if ("reportDate" %in% colnames(reports)) as.Date(reports$reportDate) else as.Date(NA),
          firstVisitDate = if ("firstVisitDate" %in% colnames(reports)) as.Date(reports$firstVisitDate) else as.Date(NA),
          reportUri = if ("reportUri" %in% colnames(reports)) reports$reportUri else NA_character_,
          reportType = if ("reportType" %in% colnames(reports)) reports$reportType else NA_character_
        )
        return(list(result_df))
      }

      if (!is.list(reports)) {
        return(list(tibble::tibble(
          linkId = character(0),
          reportDate = as.Date(character(0)),
          firstVisitDate = as.Date(character(0)),
          reportUri = character(0),
          reportType = character(0)
        )))
      }

      report_rows <- lapply(reports, function(report) {
        if (!is.list(report)) {
          return(tibble::tibble(
            linkId = NA_character_,
            reportDate = as.Date(NA),
            firstVisitDate = as.Date(NA),
            reportUri = NA_character_,
            reportType = NA_character_
          ))
        }

        tibble::tibble(
          linkId = report$linkId %||% NA_character_,
          reportDate = if (!is.null(report$reportDate)) as.Date(report$reportDate) else as.Date(NA),
          firstVisitDate = if (!is.null(report$firstVisitDate)) as.Date(report$firstVisitDate) else as.Date(NA),
          reportUri = report$reportUri %||% NA_character_,
          reportType = report$reportType %||% NA_character_
        )
      })

      result_df <- do.call(rbind, report_rows)
      list(result_df)
    }

    # Helper function to extract unpublished reports as nested dataframe
    extract_unpublished_reports_as_nested_df <- function(unpublished_reports) {
      if (is.null(unpublished_reports) || length(unpublished_reports) == 0) {
        return(list(tibble::tibble(firstVisitDate = as.Date(character(0)))))
      }

      if (is.data.frame(unpublished_reports)) {
        result_df <- tibble::tibble(
          firstVisitDate = if ("firstVisitDate" %in% colnames(unpublished_reports)) as.Date(unpublished_reports$firstVisitDate) else as.Date(NA)
        )
        return(list(result_df))
      }

      if (!is.list(unpublished_reports)) {
        return(list(tibble::tibble(firstVisitDate = as.Date(character(0)))))
      }

      report_rows <- lapply(unpublished_reports, function(report) {
        if (!is.list(report)) {
          return(tibble::tibble(firstVisitDate = as.Date(NA)))
        }
        tibble::tibble(firstVisitDate = if (!is.null(report$firstVisitDate)) as.Date(report$firstVisitDate) else as.Date(NA))
      })

      result_df <- do.call(rbind, report_rows)
      list(result_df)
    }

    # Helper function to extract regulated activities details as nested dataframe
    extract_regulated_activities_details <- function(reg_activities) {
      if (is.null(reg_activities) || length(reg_activities) == 0) {
        return(list(tibble::tibble(name = character(0), code = character(0))))
      }

      if (is.data.frame(reg_activities)) {
        result_df <- tibble::tibble(
          name = if ("name" %in% colnames(reg_activities)) reg_activities$name else NA_character_,
          code = if ("code" %in% colnames(reg_activities)) reg_activities$code else NA_character_
        )
        return(list(result_df))
      }

      if (!is.list(reg_activities)) {
        return(list(tibble::tibble(name = character(0), code = character(0))))
      }

      activity_rows <- lapply(reg_activities, function(activity) {
        if (!is.list(activity)) {
          return(tibble::tibble(name = NA_character_, code = NA_character_))
        }

        tibble::tibble(
          name = activity$name %||% NA_character_,
          code = activity$code %||% NA_character_
        )
      })

      result_df <- do.call(rbind, activity_rows)
      list(result_df)
    }

    # Helper function to extract regulated activities contacts as nested dataframe
    extract_regulated_activities_contacts <- function(reg_activities) {
      if (is.null(reg_activities) || length(reg_activities) == 0) {
        return(list(tibble::tibble(
          activity_name = character(0),
          activity_code = character(0),
          personTitle = character(0),
          personGivenName = character(0),
          personFamilyName = character(0),
          personRoles = character(0)
        )))
      }

      all_contact_rows <- list()

      if (is.data.frame(reg_activities)) {
        for (i in 1:nrow(reg_activities)) {
          activity_name <- reg_activities$name[i] %||% NA_character_
          activity_code <- reg_activities$code[i] %||% NA_character_

          contacts <- reg_activities$contacts[[i]]

          if (is.null(contacts) || nrow(contacts) == 0) {
            all_contact_rows <- c(all_contact_rows, list(tibble::tibble(
              activity_name = activity_name,
              activity_code = activity_code,
              personTitle = NA_character_,
              personGivenName = NA_character_,
              personFamilyName = NA_character_,
              personRoles = NA_character_
            )))
            next
          }

          for (j in 1:nrow(contacts)) {
            roles_value <- if ("personRoles" %in% colnames(contacts)) {
              role_data <- contacts$personRoles[[j]]
              if (!is.null(role_data)) {
                paste(role_data, collapse = ", ")
              } else {
                NA_character_
              }
            } else {
              NA_character_
            }

            contact_row <- tibble::tibble(
              activity_name = activity_name,
              activity_code = activity_code,
              personTitle = contacts$personTitle[j] %||% NA_character_,
              personGivenName = contacts$personGivenName[j] %||% NA_character_,
              personFamilyName = contacts$personFamilyName[j] %||% NA_character_,
              personRoles = roles_value
            )

            all_contact_rows <- c(all_contact_rows, list(contact_row))
          }
        }

        if (length(all_contact_rows) == 0) {
          result_df <- tibble::tibble(
            activity_name = character(0),
            activity_code = character(0),
            personTitle = character(0),
            personGivenName = character(0),
            personFamilyName = character(0),
            personRoles = character(0)
          )
        } else {
          result_df <- do.call(rbind, all_contact_rows)
        }

        return(list(result_df))
      }

      if (!is.list(reg_activities)) {
        return(list(tibble::tibble(
          activity_name = character(0),
          activity_code = character(0),
          personTitle = character(0),
          personGivenName = character(0),
          personFamilyName = character(0),
          personRoles = character(0)
        )))
      }

      for (activity in reg_activities) {
        if (!is.list(activity)) next

        activity_name <- activity$name %||% NA_character_
        activity_code <- activity$code %||% NA_character_
        contacts <- activity$contacts

        if (is.null(contacts) || length(contacts) == 0) {
          all_contact_rows <- c(all_contact_rows, list(tibble::tibble(
            activity_name = activity_name,
            activity_code = activity_code,
            personTitle = NA_character_,
            personGivenName = NA_character_,
            personFamilyName = NA_character_,
            personRoles = NA_character_
          )))
          next
        }

        if (is.data.frame(contacts)) {
          for (i in 1:nrow(contacts)) {
            roles_value <- if ("personRoles" %in% colnames(contacts)) {
              role_data <- contacts$personRoles[[i]]
              if (!is.null(role_data)) {
                if (is.list(role_data) || is.character(role_data)) {
                  paste(role_data, collapse = ", ")
                } else {
                  as.character(role_data)
                }
              } else {
                NA_character_
              }
            } else {
              NA_character_
            }

            contact_row <- tibble::tibble(
              activity_name = activity_name,
              activity_code = activity_code,
              personTitle = if ("personTitle" %in% colnames(contacts)) contacts$personTitle[i] %||% NA_character_ else NA_character_,
              personGivenName = if ("personGivenName" %in% colnames(contacts)) contacts$personGivenName[i] %||% NA_character_ else NA_character_,
              personFamilyName = if ("personFamilyName" %in% colnames(contacts)) contacts$personFamilyName[i] %||% NA_character_ else NA_character_,
              personRoles = roles_value
            )

            all_contact_rows <- c(all_contact_rows, list(contact_row))
          }
        } else {
          for (contact in contacts) {
            if (!is.list(contact)) {
              contact_row <- tibble::tibble(
                activity_name = activity_name,
                activity_code = activity_code,
                personTitle = NA_character_,
                personGivenName = NA_character_,
                personFamilyName = NA_character_,
                personRoles = NA_character_
              )
            } else {
              roles_value <- if (!is.null(contact$personRoles)) {
                if (is.list(contact$personRoles) || is.character(contact$personRoles)) {
                  paste(contact$personRoles, collapse = ", ")
                } else {
                  as.character(contact$personRoles)
                }
              } else {
                NA_character_
              }

              contact_row <- tibble::tibble(
                activity_name = activity_name,
                activity_code = activity_code,
                personTitle = contact$personTitle %||% NA_character_,
                personGivenName = contact$personGivenName %||% NA_character_,
                personFamilyName = contact$personFamilyName %||% NA_character_,
                personRoles = roles_value
              )
            }

            all_contact_rows <- c(all_contact_rows, list(contact_row))
          }
        }
      }

      if (length(all_contact_rows) == 0) {
        result_df <- tibble::tibble(
          activity_name = character(0),
          activity_code = character(0),
          personTitle = character(0),
          personGivenName = character(0),
          personFamilyName = character(0),
          personRoles = character(0)
        )
      } else {
        result_df <- do.call(rbind, all_contact_rows)
      }

      list(result_df)
    }

    # Read JSON
    x <- jsonlite::fromJSON(file, simplifyVector = TRUE)

    # Extract boolean specialisms
    boolean_specialisms <- tryCatch({
      extract_specialisms_boolean(x$specialisms, all_specialisms)
    }, error = function(e) {
      stop("Error in specialisms extraction: ", e$message)
    })

    # Extract boolean gacServiceTypes
    boolean_gac_service_types <- tryCatch({
      extract_gac_service_types_boolean(x$gacServiceTypes)
    }, error = function(e) {
      stop("Error in gacServiceTypes extraction: ", e$message)
    })

    # Extract most recent report (always extract for individual columns)
    most_recent_report <- extract_most_recent_report(x$reports)

    # Extract most recent unpublished report
    most_recent_unpublished_date <- tryCatch({
      unpub_reports <- x$unpublishedReports
      if (is.null(unpub_reports) || length(unpub_reports) == 0) {
        as.Date(NA)
      } else if (is.data.frame(unpub_reports)) {
        if (nrow(unpub_reports) == 0) {
          as.Date(NA)
        } else if ("firstVisitDate" %in% colnames(unpub_reports)) {
          visit_dates <- as.Date(unpub_reports$firstVisitDate)
          most_recent_date <- max(visit_dates, na.rm = TRUE)
          if (length(most_recent_date) > 1) {
            most_recent_date <- most_recent_date[1]
          }
          if (!is.na(most_recent_date) && length(most_recent_date) == 1) {
            most_recent_date
          } else {
            as.Date(NA)
          }
        } else {
          as.Date(NA)
        }
      } else if (is.list(unpub_reports)) {
        most_recent_date <- as.Date("1900-01-01")
        for (report in unpub_reports) {
          if (is.list(report) && !is.null(report$firstVisitDate)) {
            visit_date <- as.Date(report$firstVisitDate)
            if (length(visit_date) > 1) visit_date <- visit_date[1]
            if (!is.na(visit_date) && visit_date > most_recent_date) {
              most_recent_date <- visit_date
            }
          }
        }
        if (most_recent_date > as.Date("1900-01-01")) most_recent_date else as.Date(NA)
      } else {
        as.Date(NA)
      }
    }, error = function(e) {
      as.Date(NA)
    })

    # Extract regulated activities columns conditionally
    regulated_activities_columns <- if (extract_regulated_activities) {
      list(
        regulatedActivities_details = extract_regulated_activities_details(x$regulatedActivities),
        regulatedActivities_contacts = extract_regulated_activities_contacts(x$regulatedActivities)
      )
    } else {
      list()
    }

    # Extract reports nested column conditionally
    reports_nested_column <- if (extract_reports_nested) {
      list(reports = extract_reports_as_nested_df(x$reports))
    } else {
      list()
    }

    # Extract unpublished reports nested column conditionally
    unpublished_reports_nested_column <- if (extract_unpublished_reports_nested) {
      list(unpublishedReports = extract_unpublished_reports_as_nested_df(x$unpublishedReports))
    } else {
      list()
    }

    # Build main tibble
    tibble::tibble(
      # Basic fields (ensure single values)
      locationId = ensure_single_value(x$locationId %||% NA_character_),
      providerId = ensure_single_value(x$providerId %||% NA_character_),
      organisationType = ensure_single_value(x$organisationType %||% NA_character_),
      type = ensure_single_value(x$type %||% NA_character_),
      name = ensure_single_value(x$name %||% NA_character_),
      brandId = ensure_single_value(x$brandId %||% NA_character_),
      brandName = ensure_single_value(x$brandName %||% NA_character_),
      onspdCcgCode = x$onspdCcgCode %||% NA_character_,
      onspdCcgName = x$onspdCcgName %||% NA_character_,
      odsCcgCode = x$odsCcgCode %||% NA_character_,
      odsCcgName = x$odsCcgName %||% NA_character_,
      onspdIcbCode = x$onspdIcbCode %||% NA_character_,
      onspdIcbName = x$onspdIcbName %||% NA_character_,
      odsCode = ensure_single_value(x$odsCode %||% NA_character_),
      registrationStatus = ensure_single_value(x$registrationStatus %||% NA_character_),
      registrationDate = ensure_single_value(x$registrationDate %||% NA_character_),
      deregistrationDate = ensure_single_value(x$deregistrationDate %||% NA_character_),
      dormancy = x$dormancy %||% NA_character_,
      dormancyStartDate = x$dormancyStartDate %||% NA_character_,
      dormancyEndDate = x$dormancyEndDate %||% NA_character_,
      alsoKnownAs = x$alsoKnownAs %||% NA_character_,
      onspdLatitude = x$onspdLatitude %||% NA_real_,
      onspdLongitude = x$onspdLongitude %||% NA_real_,
      careHome = x$careHome %||% NA_character_,
      inspectionDirectorate = x$inspectionDirectorate %||% NA_character_,
      website = x$website %||% NA_character_,
      postalAddressLine1 = x$postalAddressLine1 %||% NA_character_,
      postalAddressLine2 = x$postalAddressLine2 %||% NA_character_,
      postalAddressTownCity = x$postalAddressTownCity %||% NA_character_,
      postalAddressCounty = x$postalAddressCounty %||% NA_character_,
      region = x$region %||% NA_character_,
      postalCode = x$postalCode %||% NA_character_,
      uprn = x$uprn %||% NA_character_,
      mainPhoneNumber = x$mainPhoneNumber %||% NA_character_,
      registeredManagerAbsentDate = x$registeredManagerAbsentDate %||% NA_character_,
      numberOfBeds = x$numberOfBeds %||% NA_integer_,
      constituency = x$constituency %||% NA_character_,
      localAuthority = x$localAuthority %||% NA_character_,

      # Nested date fields (extract most recent if multiple)
      lastInspection_date = extract_most_recent_date(x$lastInspection, "date"),
      lastReport_publicationDate = extract_most_recent_date(x$lastReport, "publicationDate"),

      # Location type (renamed from locationTypes_type)
      location_type = extract_from_array(x$locationTypes, "type"),

      # ADD REGULATED ACTIVITIES COLUMNS (conditional)
      !!!regulated_activities_columns,

      # ADD BOOLEAN SPECIALISMS COLUMNS
      !!!boolean_specialisms,

      # ADD BOOLEAN GAC SERVICE TYPES COLUMNS
      !!!boolean_gac_service_types,

      # Current ratings - overall ratings only (3 columns)
      currentRatings_overall_rating = safe_extract(x, "currentRatings", "overall", "rating"),
      currentRatings_overall_reportDate = safe_extract(x, "currentRatings", "overall", "reportDate"),
      currentRatings_overall_reportLinkId = safe_extract(x, "currentRatings", "overall", "reportLinkId"),

      # Current ratings - individual key question ratings (5 columns)
      currentRatings_safe = extract_individual_key_question_rating(x$currentRatings$overall, "Safe"),
      currentRatings_well_led = extract_individual_key_question_rating(x$currentRatings$overall, "Well-led"),
      currentRatings_caring = extract_individual_key_question_rating(x$currentRatings$overall, "Caring"),
      currentRatings_responsive = extract_individual_key_question_rating(x$currentRatings$overall, "Responsive"),
      currentRatings_effective = extract_individual_key_question_rating(x$currentRatings$overall, "Effective"),

      # Most recent report columns (default behavior)
      most_recent_report_linkId = if (!extract_reports_nested) {
        most_recent_report$linkId
      } else {
        NA_character_
      },
      most_recent_report_date = if (!extract_reports_nested) {
        most_recent_report$reportDate
      } else {
        as.Date(NA)
      },
      most_recent_report_uri = if (!extract_reports_nested) {
        most_recent_report$reportUri
      } else {
        NA_character_
      },
      most_recent_report_type = if (!extract_reports_nested) {
        most_recent_report$reportType
      } else {
        NA_character_
      },

      # ADD REPORTS NESTED COLUMN (conditional)
      !!!reports_nested_column,

      # Most recent unpublished report date (default behavior)
      most_recent_unpublished_report_date = if (!extract_unpublished_reports_nested) {
        most_recent_unpublished_date
      } else {
        as.Date(NA)
      },

      # ADD UNPUBLISHED REPORTS NESTED COLUMN (conditional)
      !!!unpublished_reports_nested_column

      # REMOVED: Provider inspection areas (as requested)
    )

  }, error = function(e) {
    # If any error occurs, return a minimal tibble with NA values
    warning("Error in extract_location_row for file: ", file, " - ", e$message)

    # Create minimal tibble with basic structure
    tibble::tibble(
      locationId = NA_character_,
      name = NA_character_,
      error_message = e$message,
      .rows = 1
    )
  })
}
