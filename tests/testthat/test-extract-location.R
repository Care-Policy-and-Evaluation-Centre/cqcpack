fixture <- function(fname) {
  testthat::test_path("fixtures", fname)
}

# ==============================================================================
# OUTPUT STRUCTURE
# ==============================================================================

test_that("extract_location_row returns a single-row tibble", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 1L)
})

test_that("output contains expected core columns", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  core_cols <- c(
    "locationId", "providerId", "name", "registrationStatus",
    "postalCode", "region", "careHome", "numberOfBeds",
    "onspdLatitude", "onspdLongitude"
  )
  expect_true(all(core_cols %in% names(result)))
})

test_that("specialism boolean columns are always present", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expected_specs <- c(
    "specialism_dementia",
    "specialism_caring_for_adults_over_65_yrs",
    "specialism_physical_disabilities",
    "specialism_learning_disabilities",
    "specialism_mental_health_conditions",
    "specialism_services_for_everyone"
  )
  expect_true(all(expected_specs %in% names(result)))
})

test_that("gacServiceTypes boolean columns are always present", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expected_gac <- c(
    "gacServiceTypes_nursing_homes",
    "gacServiceTypes_residential_homes",
    "gacServiceTypes_hospital",
    "gacServiceTypes_hospice"
  )
  expect_true(all(expected_gac %in% names(result)))
})

test_that("rating columns are always present", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  rating_cols <- c(
    "currentRatings_overall_rating",
    "currentRatings_safe",
    "currentRatings_well_led",
    "currentRatings_caring",
    "currentRatings_responsive",
    "currentRatings_effective"
  )
  expect_true(all(rating_cols %in% names(result)))
})

test_that("most recent report columns are always present", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  report_cols <- c(
    "most_recent_report_linkId",
    "most_recent_report_date",
    "most_recent_report_uri",
    "most_recent_report_type"
  )
  expect_true(all(report_cols %in% names(result)))
})

# ==============================================================================
# CORRECT VALUE EXTRACTION
# ==============================================================================

test_that("core identification fields parse correctly", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_equal(result$locationId, "1-104456550")
  expect_equal(result$providerId, "1-101608859")
  expect_equal(result$name, "Visitation of Our Lady Residential Care Home")
  expect_equal(result$organisationType, "Location")
  expect_equal(result$type, "Social Care Org")
})

test_that("registration fields parse correctly", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_equal(result$registrationStatus, "Registered")
  expect_equal(result$registrationDate, "2011-01-07")
})

test_that("address fields parse correctly", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_equal(result$postalAddressLine1, "57 Mount Park Road")
  expect_equal(result$postalAddressTownCity, "London")
  expect_equal(result$postalCode, "W5 2RU")
  expect_equal(result$region, "London")
  expect_equal(result$localAuthority, "Ealing")
  expect_equal(result$constituency, "Ealing Central and Acton")
})

test_that("numeric fields parse correctly", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_equal(result$numberOfBeds, 5L)
  expect_equal(result$onspdLatitude, 51.5209141)
  expect_equal(result$onspdLongitude, -0.3044582)
})

test_that("care home and dormancy fields parse correctly", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_equal(result$careHome, "Y")
  expect_equal(result$dormancy, "N")
})

test_that("ICB and CCG fields parse correctly", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_equal(result$onspdCcgCode, "E38000256")
  expect_equal(result$onspdCcgName, "NHS North West London CCG")
  expect_equal(result$onspdIcbCode, "E54000027")
  expect_equal(result$onspdIcbName, "NHS North West London Integrated Care Board")
})

test_that("lastInspection and lastReport dates extracted correctly", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_equal(result$lastInspection_date, "2022-01-18")
  expect_equal(result$lastReport_publicationDate, "2022-02-23")
})

# ==============================================================================
# SPECIALISMS AND GAC SERVICE TYPES
# ==============================================================================

test_that("correct specialism boolean is TRUE", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_true(result$specialism_caring_for_adults_over_65_yrs)
})

test_that("absent specialisms are FALSE", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_false(result$specialism_dementia)
  expect_false(result$specialism_physical_disabilities)
  expect_false(result$specialism_learning_disabilities)
  expect_false(result$specialism_mental_health_conditions)
})

test_that("correct gacServiceType boolean is TRUE", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_true(result$gacServiceTypes_residential_homes)
})

test_that("absent gacServiceTypes are FALSE", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_false(result$gacServiceTypes_nursing_homes)
  expect_false(result$gacServiceTypes_hospital)
  expect_false(result$gacServiceTypes_hospice)
})

# ==============================================================================
# RATINGS
# ==============================================================================

test_that("overall rating extracted correctly", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_equal(result$currentRatings_overall_rating, "Good")
  expect_equal(result$currentRatings_overall_reportDate, "2017-11-22")
  expect_equal(result$currentRatings_overall_reportLinkId, "07045f9e-2da3-4ab2-bdb1-660ce985cc1b")
})

test_that("all five key question ratings extracted correctly", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  expect_equal(result$currentRatings_safe,       "Good")
  expect_equal(result$currentRatings_well_led,   "Good")
  expect_equal(result$currentRatings_caring,     "Good")
  expect_equal(result$currentRatings_responsive, "Good")
  expect_equal(result$currentRatings_effective,  "Good")
})

# ==============================================================================
# REPORTS — most recent selected from six reports
# ==============================================================================

test_that("most recent report selected correctly from multiple reports", {
  result <- extract_location_row(fixture("location_1-104456550.json"))
  # Six reports in fixture; most recent is 2022-02-23
  expect_equal(result$most_recent_report_linkId, "fa6faaf6-709d-4ef8-8d1b-349ed80b1df6")
  expect_equal(result$most_recent_report_date, as.Date("2022-02-23"))
  expect_equal(result$most_recent_report_type, "Location")
})

test_that("extract_reports_nested=TRUE returns all six reports", {
  result <- extract_location_row(
    fixture("location_1-104456550.json"),
    extract_reports_nested = TRUE
  )
  expect_true("reports" %in% names(result))
  nested <- result$reports[[1]]
  expect_s3_class(nested, "data.frame")
  expect_equal(nrow(nested), 6L)
})

# ==============================================================================
# OPTIONAL PARAMETERS
# ==============================================================================

test_that("extract_regulated_activities=TRUE adds nested list columns", {
  result <- extract_location_row(
    fixture("location_1-104456550.json"),
    extract_regulated_activities = TRUE
  )
  expect_true("regulatedActivities_details" %in% names(result))
  expect_true("regulatedActivities_contacts" %in% names(result))
  nested <- result$regulatedActivities_details[[1]]
  expect_s3_class(nested, "data.frame")
  expect_equal(nested$code[1], "RA2")
})

test_that("extract_unpublished_reports_nested=TRUE adds unpublishedReports column", {
  result <- extract_location_row(
    fixture("location_1-104456550.json"),
    extract_unpublished_reports_nested = TRUE
  )
  expect_true("unpublishedReports" %in% names(result))
})

# ==============================================================================
# ERROR HANDLING
# ==============================================================================

test_that("returns a tibble even for a non-existent file", {
  expect_warning(
    result <- extract_location_row("non_existent_file.json"),
    regexp = NULL
  )
  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 1L)
})