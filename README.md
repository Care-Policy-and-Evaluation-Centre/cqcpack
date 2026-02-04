## Overview

The cqcpack is an R package that provides ready access to all Care Quality Comission (CQC) regulatory data on locations and providers. 
This is a data package where each row in the dataframe is a provider or location ID. 
In doing so, it eliminates the need for uses to make complex and multiple API calls from the publicly available CQC Syndication API.

## Key Features

1. The package provides pre-processed datasets so users can readily load complete CQC location and provider data.
2. The package automates the data compilation process by handling complex pagination, rate limiting, and JSON parsing.
3. The package incrementally updates the dataset with changes on a nightly basis.
4. The package uses GitHub Actions to version historical data by storing JSONs in a data repository called cqc-data-repo.
5. The package allows simple querying for users who wish to look up information on individual IDs using get_cqc_id_info.

## Installation
1. Ensure you have R (version 4.0.0 or higher) installed on your system.
2. Go to the [releases page](https://github.com/Care-Policy-and-Evaluation-Centre/cqcpack/releases).
3. Download the latest `cqcpack_x.x.x.tar.gz` file.
4. Replace with the path to your downloaded file to install the pacakge - `install.packages("path/to/cqcpack_0.1.2.tar.gz", repos = NULL, type = "source")`
5. Load the pacakge using `library(cqcpack)`

### Dependencies
The package will automatically install required dependencies when you install it. Key dependencies include:
- `httr` - For API requests
- `jsonlite` - For JSON parsing
- `dplyr` - For data manipulation

### Troubleshooting
If installation fails with dependency errors:

```r
# Install dependencies manually first
install.packages(c("httr", "jsonlite", "dplyr"))

# Then install cqcpack
install.packages("path/to/cqcpack_0.1.2.tar.gz", repos = NULL, type = "source")
```

