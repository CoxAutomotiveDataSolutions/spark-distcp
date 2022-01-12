# Changelog

## v0.2.3 - 2022-01-12
- Move build to Azure Devops and update dependencies
- Add cross build for scala 2.13
- Add spark 3 (3.1 and 3.2) support

## v0.2.2 - 2019-06-21

### Fixed
- Human readable byte representation sometimes trimming too many digits

## v0.2.1 - 2019-06-21

### Fixed
- Files within a partition are now batched in a consistent way using sorting
- Jobs no longer fail if batching keys are incorrect

## v0.2 - 2019-06-18

### Added
- Statistics of the copy/delete operations are now collected during the application and logged on completion

### Fixed
- Various various command-line parsing issues and increased coverage of tests to include these cases

## v0.1 - 2019-06-12

### Added
- Initial release
