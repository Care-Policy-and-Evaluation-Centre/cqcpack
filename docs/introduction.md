# CQC Data Package

2026-03-09

- [Summary](#summary)

> [!NOTE]
>
> ### Part 1: For Frontline Users
>
> > [!IMPORTANT]
> >
> > ### For End Users
> >
> > **You do NOT need to build, update, or maintain datasets.** This
> > package uses GitHub Actions to automatically update dataframes daily
> > from the CQC API. You can simply install the package from GitHub and
> > access the automatically-updated provider, location or merged
> > datasets. You can also use `get_cqc_id_info()` to query any CQC
> > provider or location. The datasets cover both location-level data
> > (individual care facilities and services) and provider-level data
> > (organizations that run these services).
>
> ### What is CQC?
>
> The Care Quality Commission (CQC) is the independent regulator of
> health and social care services in England. It monitors, inspects, and
> rates services to ensure they meet fundamental standards of quality
> and safety. The CQC oversees a wide range of care providers including
> hospitals, GP practices, care homes, dental practices, ambulance
> services, and mental health services. Their ratings system
> (Outstanding, Good, Inadequate and Needs Improvement) helps the public
> make informed choices about their care while also attempting to
> encourage improvement across the sector.
>
> ### What Data is Available?
>
> Once the package is loaded, three ready-to-use datasets are available
> immediately:
>
> | Dataset       | Description                                             |
> |---------------|---------------------------------------------------------|
> | `location_df` | Individual care facilities and services                 |
> | `provider_df` | Organisations that run those services                   |
> | `merged_df`   | Combined dataset linking providers with their locations |
>
> For most users, `merged_df` is the most useful as it contains both
> location and provider information in one place.
>
> ## Quick Start: What You Need to Know
>
> ### Installation
>
> The cqcpack package is available on GitHub and comes with
> daily-updated datasets on providers, locations and a combination of
> the two:
>
> ``` r
> # Install remotes if you don't have it
> install.packages("remotes")
>   
> # Install from GitHub
> remotes::install_github("Care-Policy-and-Evaluation-Centre/cqcpack", force = TRUE)
>
> # Load the package 
> library(cqcpack)
> ```
>
> **Note:** These datasets are pre-processed dataframes updated daily
> from the CQC API. When you load the package, you will always have
> access to data from within the last 24 hours.
>
> > [!IMPORTANT]
> >
> > **Windows Only.** This package has currently only been tested on
> > Windows. Mac users may experience issues during installation where R
> > and data files are cloned as blank files. We are investigating this
> > and hope to add Mac support in a future release. If you are on a Mac
> > and encounter this issue, please raise it on our [GitHub Issues
> > page](https://github.com/Care-Policy-and-Evaluation-Centre/cqcpack/issues).
>
> ### Accessing Pre-Built Datasets
>
> The package comes with ready-to-use datasets. You can access them
> directly without any building or updating:
>
> ``` r
> # Load your preferred dataset
> merged_df
>         
> # Check the data 
> dim(merged_df) 
> head(merged_df, 5)  
> ```
>
> ### Analysing ratings in your area
>
> ``` r
> # Summary of ratings
> table(merged_df$location_currentRatings_overall_rating)
>
> # Filter for Good-rated services
> good_rated <- merged_df[merged_df$location_currentRatings_overall_rating == "Good", ]
> head(good_rated, 5)
>
> ratings <- as.data.frame(table(Rating = merged_df$location_currentRatings_overall_rating))
> ratings$Percentage <- paste0(round(ratings$Freq / sum(ratings$Freq) * 100, 1), "%")
> colnames(ratings) <- c("Rating", "Count", "Percentage")
>
> knitr::kable(
>   ratings,
>   caption = "Breakdown of CQC overall ratings across all registered services",
>   align = c("l", "r", "r"),
>   format.args = list(big.mark = ",")
> )
> ```
>
> ### Sample Data
>
> The table below shows the first 6 rows of the merged dataset,
> displaying some relevant columns:
>
> ``` r
> # Select the most useful columns to display
> sample_cols <- c(
>   "location_name",
>   "location_type",
>   "location_region",
>   "location_postalCode",
>   "location_registrationStatus",
>   "location_currentRatings_overall_rating",
>   "provider_name"
> )
>
> kable(
>   head(merged_df[, sample_cols], 6),
>   col.names = c(
>     "Location Name",
>     "Type",
>     "Region",
>     "Postcode",
>     "Status",
>     "Overall Rating",
>     "Provider Name"
>   ),
>   caption = "Sample of the CQC merged dataset"
> )
> ```
>
> ### Querying CQC IDs
>
> The primary function you will use is get_cqc_id_info(). This retrieves
> detailed information for any specific CQC provider or location ID.
>
> ``` r
> # For a location ID
> location_info <- get_cqc_id_info("1-10000302982", id_type = "location")
>
> # For a provider ID
> provider_info <- get_cqc_id_info("1-102642938", id_type = "provider")
>
> print(location_info)
> print(provider_info)
> ```

> [!NOTE]
>
> ### Part 2: Background Technical Information
>
> > [!WARNING]
> >
> > ### Advanced Users Only
> >
> > The following sections describe the internal workings of the
> > package. **Regular users do not need to use these functions or
> > maintain any data.** The package is automatically updated via GitHub
> > Actions, and the data is pre-built and maintained for you.
>
> ## How This Package Works
>
> ### Data Source
>
> The package retrieves data directly from the official CQC API
> endpoints through an automated GitHub Actions workflow. The repository
> contains pre-processed dataframes that are automatically updated daily
> via scheduled runs. These dataframes include comprehensive datasets
> about providers and locations, with their registration details,
> ratings, inspection dates, and service characteristics.
>
> The raw JSON data from the CQC API is stored in a separate repository:
> [cqc-data-repo](https://github.com/Care-Policy-and-Evaluation-Centre/cqc-data-repo.git),
> while the processed dataframes live in the package itself. This
> automation means users always have access to recent data without
> needing to call the API themselves or perform any maintenance.
>
> ### What this Package Covers
>
> The `cqcpack` package provides a complete toolkit for accessing and
> managing CQC regulatory data. The package handles the full data
> lifecycle including initial bulk data collection, caching for
> efficiency, incremental updates to keep data current, and merging
> capabilities to create comprehensive datasets linking providers with
> their locations.
>
> ### Why this Approach?
>
> Working directly with the CQC API requires handling pagination, rate
> limits, JSON parsing, and wrangling complex data structures. This
> package eliminates these challenges by providing functions that handle
> all the complexity behind the scenes. It offers:
>
> - Bulk data collection with local caching to avoid redundant API calls
>
> - Automated incremental updates that capture only changes since the
>   last data pull
>
> - Built-in data transformation that converts nested JSON structures
>   into analysis-ready dataframes
>
> - The ability to merge provider and location data for comprehensive
>   analysis
>
> ## Technical Functions (for Package Maintainers)
>
> ### Caching Functions
>
> These functions handle the initial data collection and storage:
>
> - `cache_location_ids()` - Downloads and caches all location
>   identifiers
>
> - `cache_location_jsons()` - Fetches and stores detailed JSON data for
>   all locations
>
> - `cache_provider_ids()` - Downloads and caches all provider
>   identifiers
>
> - `cache_provider_jsons()` - Fetches and stores detailed JSON data for
>   all providers
>
> ### Data Building Functions
>
> These functions construct dataframes from cached data:
>
> - `build_location_df()` - Constructs a dataframe from cached location
>   JSON files
>
> - `build_provider_df()` - Constructs a dataframe from cached provider
>   JSON files
>
> ### Data Integration Functions
>
> - `merge_provider_location()` - Combines provider and location
>   datasets into a unified dataframe
>
> ### Update Functions
>
> These functions maintain currency of the datasets:
>
> - `get_incremental_changes()` - Identifies changes since the last data
>   collection
>
> - `update_location_dataset()` - Updates location data with recent
>   changes
>
> - `update_provider_dataset()` - Updates provider data with recent
>   changes
>
> ### Query Functions
>
> - `get_cqc_id_info()` - Retrieves detailed information for specific
>   provider or location IDs (PRIMARY USER FUNCTION)
>
> ## Automated Update System
>
> The package uses GitHub Actions to automatically maintain current
> data:
>
> 1.  **Scheduled Updates**: Runs daily to check for new data from the
>     CQC API
>
> 2.  **Incremental Updates**: Uses `get_incremental_changes()`,
>     `update_location_dataset()`, and `update_provider_dataset()` to
>     fetch only new or changed records
>
> 3.  **Data Storage**:
>
>     - Processed dataframes - package’s `data/` folder
>
>     - Raw JSON files -
>       [cqc-data-repo](https://github.com/Care-Policy-and-Evaluation-Centre/cqc-data-repo.git)
>
> 4.  **Automatic Deployment**: Updated dataframes are automatically
>     committed to the package repository
>
> To get the latest data, simply reinstall the package by running the
> install command again.
>
> ## Manual Update Workflow (Optional)
>
> For troubleshooting or manual updates, here’s the process that GitHub
> Actions runs automatically:
>
>     library(cqcpack)  
>
>     # Get incremental changes from CQC API 
>     changes <- get_incremental_changes() 
>     str(changes, max.level = 2)  
>
>     # Update datasets with recent changes 
>     update_location_dataset() 
>     update_provider_dataset()  
>
>     # Create updated merged dataset 
>     merged_data <- merge_provider_location()  
>
>     # Verify the updates 
>     tail(merged_data[order(merged_data$date), c("locationName", "date")], 3) 

## Summary

**For end users:** Install the package from GitHub and use
`get_cqc_id_info()` or `merge_provider_location()`. The data is
automatically kept up-to-date via GitHub Actions. That’s it!

**For package maintainers:** GitHub Actions handles daily updates
automatically. The workflow uses incremental update functions to fetch
new data and stores processed dataframes in the package and raw JSONs in
the
[cqc-data-repo](https://github.com/Care-Policy-and-Evaluation-Centre/cqc-data-repo.git).
