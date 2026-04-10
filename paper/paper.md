
---
title: 'cqcpack: R Package for Accessing Care Quality Commission Data'
tags:
- R
- health informatics
- open source data
- care quality commission
- NHS
date: "07 April 2026"
output: pdf_document
authors:
- name: Ritwika Das
  orcid: "0009-0009-0352-6167"
  affiliation: 1
bibliography: "paper.bib"
affiliations:
- name: London School of Economics and Political Science, United Kingdom
  index: 1
---

## Summary

`cqcpack` (v0.1.0) is an R package that provides an automated workflow to access Care Quality Commission (CQC) data in bulk from their publicly accessible Syndication API [@Das2026]. CQC is an independent regulator of health and social care services in England. CQC ratings are widely recognised as the gold standard for assessing the quality of such provisions and are routinely consulted by a broad range of stakeholders such as service users and their families, local authorities, commissioners and researchers when they need to determine the standard of a registered provider. In addition to publishing rating information and detailed provider reports, the CQC operates a publicly accessible Syndication API releasing corresponding information on registration status, regulated activities, and many other service characteristics. Such granular data, though publicly accessible, presents a significant technical barrier for those seeking to up to date CQC datasets which has highlighted the need for a more streamlined solution.

The package addresses this by retrieving all location and provider IDs from list endpoints, fetching each ID's JSON data, and converting responses into data frames ready for analysis. The package eliminates technical barriers and democratises access to regulatory data for researchers and policy analysts regardless of programming background.

## Statement of Need

By design, the CQC API does not support bulk downloads. As such, users needed to identify required IDs and request data individually for each one. For users requiring data across multiple or all available IDs, this process becomes time consuming, taking hours up to a full day to complete. The package solves this problem through four main mechanisms i.e., automating API calls, caching the JSONs, parsing JSONs into a dataset and incrementally updating changes to the dataset. This workflow generates three .rda files containing location data, provider data, and a merged dataset combining both.

As services open, close, or transfer between providers, CQC data undergoes continuous changes which makes tracking these updates manually, impractical. Therefore, the package's novelty lies in its automated versioning system which uses GitHub Actions to update the package data, nightly. It does so by querying the changes endpoint to incrementally update data rows with modified records, where each row corresponds to a location or provider ID. To make sure old data is not lost the package archives each JSON snapshot in a separate data repository called cqc-data-repo [@Das2025]. This allows users to readily access historical snapshots without manually re-running the package to recreate older versions.

## State of the Field

There are a few existing R packages that provide access to health related administrative datasets in the UK. The `fingertipsR` package [@fingertipsR] provides access to Public Health England's Fingertips repository, which contains a wide range of population and public health indicators for England. Similarly, `NHSRdatasets` [@NHSRdatasets] provides synthetic and open licenced NHS and other healthcare related datasets designed for training . However, neither of these packages target the domain of social care. Therefore, CQC data is distinct in combining provider level quality ratings, service type classifications, geographic information, and inspection history at the level of individual registered locations. `cqcpack` therefore fills a clear gap in the provision of health data access tools for R users.

## Software Design

`cqcpack` operates by first handling the API's pagination by iterating over location and provider IDs until all records have been retrieved. The full lists of active location and provider IDs are then scraped from the API, after which all downloaded IDs and their corresponding JSONs are written into a date stamped folder. The initial bulk download can be found in `cqc-data-repo`. The package then converts the raw JSON files into a table by parsing each JSON into a tibble and then binding the rows into a single dataset. A merged dataset is also produced by linking providers with their locations. In addition, the package contains simple functions such as `get_location_info()` and `get_provider_info()` to query CQC data directly in R, allowing users to look up specific organisations by ID from the cached dataset without making API calls.

In order to get the latest updates, the package identifies the latest date present in the existing dataset and queries the CQC changes endpoint for the intervening period, caching only the relevant JSONs. Although nightly automated updates maintain current data in the repository, users can also manually trigger an update to fetch the latest changes and recompile the merged dataset.

## Research Impact Statement

This package contributes to improved data access by making CQC data available to any R user, automation by eliminating manual API calls and time consuming JSON transformations, and reproducibility by maintaining version controlled datasets to promote replication. It provides immediate access to regulatory data covering all providers and locations across England registered with the CQC, and facilitates future studies on care quality trends, service characteristics, policy evaluation, public health research, service accessibility, and much more.

In order to use the package, users must first install it from its GitHub repository by following the instructions set out in the README. Once loaded, users can immediately filter and analyse service types, ratings, geographic locations, and regulatory characteristics without writing any API code. Some examples of use case have been provided in the package documentation. 

## AI Usage Disclosure

Claude (Anthropic, claude-sonnet-4-6) was used to assist with spell checking and formatting portions of this manuscript to ensure adherence to JOSS submission guidelines.

## Acknowledgements

The author thanks the Care Quality Commission for maintaining a publicly accessible API and for the open data principles that make this work possible. This work was supported by the Care Policy and Evaluation Centre at the London School of Economics and Political Science, which funded the development of this package.

## References
