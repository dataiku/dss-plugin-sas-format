# Changelog

## Version 1.6.0 - Feature release - 2026-10-21

- Add an opt-in original SAS schema option to the Pandas reader for SAS7BDAT and XPORT files, preserving numeric, character, and recognized date/datetime types.
- Keep existing schema detection and row values unchanged when the option is absent or disabled.

## Version 1.5.1 - Bugfix release - 2025-07-07

- Fix detection of schema of .xport files

## Version 1.5.0 - Bugfix release - 2025-06-11

- Fix .xport format handling

## Version 1.4.0 - Bugfix release - 2025-02-18

- Improved compatibility by using more recent Pandas version
- Improved compatibility when not using "Dump to file" option
- Removed support for Python < 3.9
- Added support for Python 3.11 and 3.12

## Version 1.3.1 - Bugfix release - 2023-07-13
- Fixed bug in formatter config file

## Version 1.3.0 - Bugfix release - 2023-04-27
- ✨ Added support for Python 3.7, 3.8, 3.9, 3.10

## Version 1.2.1 - Feature and bug fix release - 2021-08-12

- fix broken "dump to file" option
- added a new format that uses the sas7bdat package

## Version 1.2.0 - Feature release - 2021-04-09

- added support of DSS 9.0.1 Seekable streams
