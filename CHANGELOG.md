<!-- markdownlint-disable MD024 -->
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Fixed

## [2.1.0] - 2019-12-12

### Added

- `vectorpipe.examples`: VectorPipe examples moved from https://github.com/azavea/osmesa
- `VectorPipe.defaultSparkSessionWithJTS` method to construct a VectorPipe tailored `SparkSession`. Users with more complicated use cases will still want to manually construct their own session.

## [2.0.0] - 2019-11-29

This is the first release to depend on GeoTrellis 3.0.

### Changed

- Streaming sources now fallback to the current remote sequence if no database
  checkpoint or option can be found
- Depend on Spark 2.4.4
- Depend on GeoTrellis 3.1.0

## [1.1.0] - 2019-09-26

### Added

- `useCaching` option to VectorPipe.Options allows for persisting to disk.
  Helps avoid repeated computations.
- Functions for converting sequence numbers to timestamps and back for both
  changeset replications and augmented diff replications. See `ChangesetSource`
  and `AugmentedDiffSource` in `vectorpipe.sources`.

### Changed

- Improved empty geometry handling in UDFs

### Fixed

## [1.0.0] - 2019-07-09

### Added

- RELEASING.md - Instructions for releasing new versions of this project
- Support for semicolon-delimited tag values in UDFs, e.g. `shop=bakery;dairy`
- Support for `nds` in augmented diff GeoJSON (matching
  [`osm-replication-streams@^0.7.0`](https://github.com/mojodna/osm-replication-streams/tree/v0.7.0)
  output)
- "Uninteresting" tags are dropped when processing OSM inputs; this will result
  in fewer point features being generated (as those nodes previously had tags
  applied).

### Changed

- Sync with [id-area-keys@2.13.0](https://github.com/osmlab/id-area-keys/blob/v2.13.0/areaKeys.json) for determining area-ness of a way.
- Fetch gzipped augmented diff JSON (produced by [overpass-diff-publisher](https://github.com/mojodna/overpass-diff-publisher))
- Preserve the last-known coordinates of deleted nodes
- Better handling of falsy boolean values in tag UDFs
- Adds `riverbank`, `stream_end`, `dam`, `weir`, `waterfall`, and `pressurised`
  to the list of waterway features
- Populates `nds` and `members` for deleted elements from the previous version

### Fixed

- Resolve commons-io deprecation warnings
- Convert coordinates to Doubles (expected by VP internals) when pre-processing

## [1.0.0-RC3] - 2019-04-24

### Fixed

- Mark all logger vals and some UDF vals as @transient lazy to avoid Spark serialization issues
- Properly strip leading and trailing slashes from S3 URIs when exporting vector tiles
