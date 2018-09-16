# Melon

## Abstract

Project Melon creates a database abstraction from arbitrary structured files to enable the use of SQL for reading, combining and manipulating data.

## Goals

1. Maintaining data files (e.g. CSV, XML, etc.) in a structured environment instead of a text editor (YES: read AND write - it is not an importer!)
2. Simple API to enable support for all kind of (custom) file formats
3. Simple but flexible configuration

## State

### v0.1.0
- JDBC driver backed by [H2 in memory database](https://github.com/h2database/h2database)
- support for .csv, .xml and .properties files
- YAML configuration with properties section for custom extensions

## Disclaimer

This project is in an early state - please use with caution and make backups of your files.

**Please note:** Changing data via JDBC may cause the source file to be updated in a different style than the original.
