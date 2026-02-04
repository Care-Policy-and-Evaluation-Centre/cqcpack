## Overview

The cqcpack is an R package that provides ready access to all Care Quality Comission (CQC) regulatory data on locations and providers. 
This is a data package where each row in the dataframe is a provider or location ID. 
In doing so, it eliminates the need for uses to make complex and multiple API calls from the publicly available CQC Syndication API.

## Key Features

1. The package provides pre-processed datasets so users can readily load complete location and provider data.
2. The package automates the data compilation process by handling complex pagination, rate limiting, and JSON parsing.
3. The package incrementally updates the dataset with changes on a nightly basis.
4. The package uses GitHub Actions to version historical data by storing JSONs in a data repository called cqc-data-repo.
5. The package allows simple querying for users who wish to look up information on individual IDs.