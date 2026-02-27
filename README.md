# sqlfs

## Overview

A CLI tool for creating a database from a set of defined schemas that is populated by yaml/toml/json files from a given directory.

### Commands

| Command                        | Action                                                                 |
| ------------------------------ | ---------------------------------------------------------------------- |
| `sqlfs json-schema <root>`     | Writes a json schema from the schema definitions                       |
| `sqlfs build -o <file> <root>` | Builds a file that contains the entire database from the static files  |
| `sqlfs serve <root>`           | Runs a SQL server containing the entire database from the static files |
| `sqlfs config-schema <root>`   | Writes a json schema to validate the `sqlfs.yaml` file                 |

#### `json-schema`

Parse the `schema.dbml` and generate a single json schema file that can be used to parse yaml/json/etc files (e.g. via vs code or CLI validation).

On error, it should return a non-zero exit code.

##### Parameters

- `root` (required) - the root directory that contains the static files to populate the database
- `outfile-file` - optional location of the file to write the json schema to, if none provided it is written to `stdout`

#### `build`

1. Parse the `schema.dbml` and generate a the SQL structure that corresponds.
2. Iterates through all supported files (exlucing `schema.dbml` and `sqlfs.yaml`) in the root directory (and its decendants)

- validate that it matches the schema
- insert it into the database with appropriate foreign keys

3. save the resulting file at the appropriate location

On error, it should return a non-zero exit code.

##### Parameters

- `root` (required) - the root directory that contains the static files to populate the database
- `outfile-file` (required) - location of the file to write the populated database to
- `invalid` - optionally sets the behavior for a file that does not pass schema validation: `silent`, `warn`, `fail` (default)

#### `serve`

1. Watch all supported files (including `schema.dbml`) for changes, intelligently re-run the following as necessary
2. Parse the `schema.dbml` and generate a the SQL structure that corresponds.
3. Iterates through all supported files (exlucing `schema.dbml` and `sqlfs.yaml`) in the root directory (and its decendants)

- validate that it matches the schema
- insert it into the database with appropriate foreign keys

4. save the resulting file at the appropriate location
5. Configure and run a SQL server to serve this file

On error, it should return a non-zero exit code.

If the environment variables `SQLFS_USERNAME` and `SQLFS_PASSWORD` are set, the server should require those credentials. If not, then all connections shall be accepted.

The database is read only.

##### Parameters

- `root` (required) - the root directory that contains the static files to populate the database
- `outfile-file` (required) - location of the file to write the populated database to
- `invalid` - optionally sets the behavior for a file that does not pass schema validation: `silent`, `warn` (default), `fail`
- `port` - the port to run the server on

### Config file

In the root of the static files directory, there is an optional file `sqlfs.yaml`.

It specifies:

- The column names for the standard set of columns (e.g. `path`, `ulid`)
- The name/location of the `schema.dbml` file
- The invalid behavior (the CLI argument overrides this)
- The SQL server's port (the CLI argument overrides this)
- The SQL server's credential vairables (defaults: `SQLFS_USERNAME` and `SQLFS_PASSWORD`)

### Schema definition

In the root of the static files directory, there is a file `schema.dbml` in [dbml](https://dbml.dbdiagram.io/) format.

#### Standard columns

In addition to all fields spcified in the `schema.dbml` file, the following fields are also added:

- `__path__` - is the relative path from the root of the static files directory for the file on which that row is based on
- `__created_at__` - the filesystem's timestampf for when the file was created
- `__modified_at__` - the filesystem's timestampf for when the file was modified
- `__checksum__` - the md5 checksum of the file
- `__ulid__` - a ULID that is unique for this file (and build) based on when the file was created

These fields can be referenced in `schema.dbml` for entity relationships.

Note: Do not include these fields in the json schema

### Static Files

Static files are in one of the following human readable formats:

- YAML
- TOML
- JSON (with comments and trailing spaces, e.g. via HJSON / JSON5 )
- XML
- plist

Note: comments in these files will be ignored and will not be included in the resulting database

### Database

The only supported database output format is SQLite. In the future, the list may include: PostgreSQL, MySQL, MSSQL, and Oracle.

## Developing

### Setup

Use a devcontainer.

### Commands

| Command                  | Action                                                      |
| ------------------------ | ----------------------------------------------------------- |
| `just build`             | Build the CLI for release                                   |
| `just compile`           | Build the CLI for local execution                           |
| `just dev`               | Build the CLI for local execution while watching the source |
| `just release <version>` | Release the CLI with given `<version>`                      |
| `just test`              | Test the code                                               |
