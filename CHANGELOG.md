# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
 
### Added

- RELEASING.md - Instructions for releasing new versions of this project
- Sync with [id-area-keys@2.13.0](https://github.com/osmlab/id-area-keys/blob/v2.13.0/areaKeys.json) for determining area-ness of a way.
- Resolve commons-io deprecation warnings

## [1.0.0-RC3] - 2019-04-24
 
### Fixed

- Mark all logger vals and some UDF vals as @transient lazy to avoid Spark serialization issues
- Properly strip leading and trailing slashes from S3 URIs when exporting vector tiles

