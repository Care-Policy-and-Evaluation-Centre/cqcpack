fixture <- function(fname) {
  testthat::test_path("fixtures", fname)
}

# ==============================================================================
# OUTPUT STRUCTURE
# ==============================================================================

test_that("extract_provider_row returns a single-row tibble", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 1L)
})

test_that("output contains expected core columns", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  core_cols <- c(
    "providerId", "name", "organisationType", "ownershipType",
    "registrationStatus", "registrationDate",
    "postalCode", "region", "onspdLatitude", "onspdLongitude",
    "locationIds"
  )
  expect_true(all(core_cols %in% names(result)))
})

test_that("rating columns are always present", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  rating_cols <- c(
    "currentRatings_overall_rating",
    "current_keyQuestionRating_Caring",
    "current_keyQuestionRating_Effective",
    "current_keyQuestionRating_Responsive",
    "current_keyQuestionRating_Safe",
    "current_keyQuestionRating_Well_led"
  )
  expect_true(all(rating_cols %in% names(result)))
})

test_that("has_contacts_data flag column present by default", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  expect_true("has_contacts_data" %in% names(result))
  expect_type(result$has_contacts_data, "logical")
})

# ==============================================================================
# CORRECT VALUE EXTRACTION
# ==============================================================================

test_that("core identification fields parse correctly", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  expect_equal(result$providerId, "1-10000227676")
  expect_equal(result$name, "Healthcare Employment Partners Ltd")
  expect_equal(result$organisationType, "Provider")
  expect_equal(result$ownershipType, "Organisation")
  expect_equal(result$type, "Social Care Org")
})

test_that("registration fields parse correctly", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  expect_equal(result$registrationStatus, "Deregistered")
  expect_equal(result$registrationDate, "2021-11-05")
  expect_equal(result$deregistrationDate, "2024-10-16")
})

test_that("address fields parse correctly", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  expect_equal(result$postalAddressLine1, "The Grange")
  expect_equal(result$postalAddressTownCity, "Romsey")
  expect_equal(result$postalCode, "SO51 0AE")
  expect_equal(result$region, "South East")
  expect_equal(result$localAuthority, "Hampshire")
  expect_equal(result$constituency, "Romsey and Southampton North")
})

test_that("numeric and coordinate fields parse correctly", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  expect_equal(result$onspdLatitude,  51.0331411)
  expect_equal(result$onspdLongitude, -1.5247069)
})

test_that("companies house number and ODS code parse correctly", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  expect_equal(result$companiesHouseNumber, "12904643")
  expect_equal(result$odsCode, "C3JN")
})

test_that("ICB fields parse correctly", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  expect_equal(result$onspdIcbCode, "E54000042")
  expect_equal(result$onspdIcbName, "NHS Hampshire and Isle of Wight Integrated Care Board")
})

test_that("locationIds collapsed to semicolon-separated string", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  ids <- strsplit(result$locationIds, "; ")[[1]]
  expect_length(ids, 2L)
  expect_true("1-11958143117" %in% ids)
  expect_true("1-12415390500" %in% ids)
})

# ==============================================================================
# NA HANDLING — this provider has no ratings, reports, or contacts
# ==============================================================================

test_that("ratings are NA when absent", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  expect_true(is.na(result$currentRatings_overall_rating))
  expect_true(is.na(result$current_keyQuestionRating_Safe))
  expect_true(is.na(result$current_keyQuestionRating_Caring))
  expect_true(is.na(result$current_keyQuestionRating_Well_led))
})

test_that("lastInspection and lastReport dates are NA when absent", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  expect_true(is.na(result$lastInspection_date))
  expect_true(is.na(result$lastReport_publicationDate))
})

test_that("has_contacts_data is FALSE when contacts empty", {
  result <- extract_provider_row(fixture("provider_1-10000227676.json"))
  expect_false(result$has_contacts_data)
})

# ==============================================================================
# OPTIONAL PARAMETERS
# ==============================================================================

test_that("add_contacts_flag=FALSE removes has_contacts_data column", {
  result <- extract_provider_row(
    fixture("provider_1-10000227676.json"),
    add_contacts_flag = FALSE
  )
  expect_false("has_contacts_data" %in% names(result))
})

test_that("extract_contacts_nested=TRUE adds contacts list column", {
  result <- extract_provider_row(
    fixture("provider_1-10000227676.json"),
    extract_contacts_nested = TRUE
  )
  expect_true("contacts" %in% names(result))
  nested <- result$contacts[[1]]
  expect_s3_class(nested, "data.frame")
  # contacts array is empty in this fixture
  expect_equal(nrow(nested), 0L)
})

test_that("extract_inspection_categories=TRUE adds inspectionCategories list column", {
  result <- extract_provider_row(
    fixture("provider_1-10000227676.json"),
    extract_inspection_categories = TRUE
  )
  expect_true("inspectionCategories" %in% names(result))
  nested <- result$inspectionCategories[[1]]
  expect_s3_class(nested, "data.frame")
  expect_equal(nrow(nested), 1L)
  expect_equal(nested$code[1], "S2")
  expect_equal(nested$name[1], "Community based adult social care services")
})

test_that("extract_regulated_activities=TRUE adds regulatedActivities list column", {
  result <- extract_provider_row(
    fixture("provider_1-10000227676.json"),
    extract_regulated_activities = TRUE
  )
  expect_true("regulatedActivities" %in% names(result))
  nested <- result$regulatedActivities[[1]]
  expect_s3_class(nested, "data.frame")
  # regulatedActivities is empty in this fixture
  expect_equal(nrow(nested), 0L)
})

test_that("extract_reports_nested=TRUE adds reports list column", {
  result <- extract_provider_row(
    fixture("provider_1-10000227676.json"),
    extract_reports_nested = TRUE
  )
  expect_true("reports" %in% names(result))
  nested <- result$reports[[1]]
  expect_s3_class(nested, "data.frame")
  # no reports in this fixture
  expect_equal(nrow(nested), 0L)
})

# ==============================================================================
# ERROR HANDLING
# ==============================================================================

test_that("returns a tibble even for a non-existent file", {
  expect_warning(
    result <- extract_provider_row("non_existent_file.json"),
    regexp = NULL
  )
  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 1L)
})