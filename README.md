[![Build Status](https://travis-ci.org/melon-db/melon.svg?branch=master)](https://travis-ci.org/melon-db/melon)
[![Coverage Status](https://coveralls.io/repos/github/melon-db/melon/badge.svg?branch=master)](https://coveralls.io/github/melon-db/melon?branch=master)

# Melon

## Abstract

Project Melon creates a database abstraction for arbitrary structured files to enable the use of SQL for reading, combining and manipulating data.

## Goals

1. Maintaining data files (e.g. CSV, XML, etc.) in a proper environment that supports structured data instead of using a plain text editor... YES: read AND write - it is not an importer!
2. Simple API to enable support for all kind of (custom) file formats
3. Simple but flexible configuration

## State

### v0.10.0

- use JOOQ as SQL abstraction layer
- storage parameter 'storage-adapter' to manually enforce certain storage adapter class

### v0.9.0

- MelonStorage: enables editing data across several files within one table
- FileWatcher for better data sync

### v0.6.0

- XML supports values as attributes
- properties are by default case-insensitive
- config property 'storage.location' is now 'storage.path'

### v0.5.0

- Column entity got a reference fields
- foreign key definition and creation

### v0.4.0

- separate wrapper and standalone driver but re-use connection class
- rename melon-standalone to melon-db

### v0.3.0

- project structure rework *again*
- [Travis CI](https://travis-ci.org/SeeSharpSoft/melon)
- [ShadowJar](https://github.com/johnrengelman/shadow)

### v0.2.0

- support for arbitrary databases as backbone
- support for Html files
- project structure rework

### v0.1.1

- support for AUTO COMMIT ~(currently manual commit required to update files with changes)~

### v0.1.0

- JDBC driver backed by [H2 in memory database](https://github.com/h2database/h2database)
- succesfully tested with JDBC supporting database tools as [DBeaver](https://dbeaver.io/) (community edition available) and [DataGrip](https://www.jetbrains.com/datagrip/) (commercial, integrated in [IntelliJ IDEA Ultimate](https://www.jetbrains.com/idea/))
- support for .csv, .xml and .properties files
- YAML configuration with properties section for custom extensions
- configuration of tables (and their source), columns and views

## Next steps

- documentation with examples
- support for JSON and Excel files
- extended schema definition (e.g. value transformation & data types)
- smarter data sync (e.g. check for existing entities, update only what/when necessary, merge with existing data, file watcher, ...)
- support for data relation (e.g. combine data from different files into one *editable* table)

## Disclaimer

This project is in an early state - please use with caution and make backups of your files.

**Please note:** Changing data via JDBC may cause the source file to be updated in a different style than the original.
