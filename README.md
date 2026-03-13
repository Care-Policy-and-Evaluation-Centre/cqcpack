## Overview

The cqcrpack is an R package that provides ready access to all Care Quality Commission (CQC) regulatory data on locations and providers.
This is a data package containing three pre-processed and up-to-date dataframes. 
The package contains a dataset for providers where each row is a provider, and similarly a 
second dataset for locations where each row is a location. Lastly, the package comes with a third dataset where 
each row is a unique combination of provider and location, since a single provider may have multiple locations. 
In developing this package I eliminate the need for users to write complex code to make multiple API calls from the 
publicly available CQC Syndication API.

## Key Features

1. The package provides pre-processed datasets so users can readily load complete CQC location and provider data.
2. The package automates the data compilation process by handling complex pagination, rate limiting, and JSON parsing.
3. The package incrementally updates the dataset with changes on a nightly basis.
4. The package uses GitHub Actions to version historical data by storing JSONs in a data repository called cqc-data-repo.
5. The package allows simple querying for users who wish to look up information on individual IDs using get_cqc_id_info.

## Installation
1. Ensure you have R (version 4.0.0 or higher) installed on your system.
2. Run ```remotes::install_github("Care-Policy-and-Evaluation-Centre/cqcpack")``` to install the package.
3. Run ```library(cqcpack)``` to load the package. 
4. To load in your preferred dataset you can run ```provider_df```, ```location_df``` or ```merged_df```.
5. In order to access the latest dataset each day, reinstall the package to retrieve the updated datasets.

