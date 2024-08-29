# Build Your Own SQLite

This is my solution in Go for the
["Build Your Own SQLite" Challenge](https://codecrafters.io/challenges/sqlite).

In this challenge, I have built a barebones SQLite implementation that supports
basic SQL queries by reading the raw [SQLite's file format](https://www.sqlite.org/fileformat.html),
filtering using indexes and traversing the [stored B-trees](https://jvns.ca/blog/2014/10/02/how-does-sqlite-work-part-2-btrees/)
structure and more.

# Stages

Final implementation passes all stages of [sqlite-tester v47](https://github.com/codecrafters-io/sqlite-tester/releases/tag/v47):

- [x] Print page size
- [x] Print number of tables
- [x] Print table names
- [x] Count rows in a table
- [x] Read data from a single column
- [x] Read data from multiple columns
- [x] Filter data with a WHERE clause
- [x] Retrieve data using a full-table scan
- [x] Retrieve data using an index

# Sample Databases

To make it easy to test queries locally, we've added a sample database in the
root of this repository: `sample.db`.

This contains two tables: `apples` & `oranges`. You can use this to test your
implementation for the first 6 stages.

You can explore this database by running queries against it like this:

```sh
$ sqlite3 sample.db "select id, name from apples"
1|Granny Smith
2|Fuji
3|Honeycrisp
4|Golden Delicious
```

There are two other databases that you can use:

1. `superheroes.db`:
   - This is a small version of the test database used in the table-scan stage.
   - It contains one table: `superheroes`.
   - It is ~1MB in size.
1. `companies.db`:
   - This is a small version of the test database used in the index-scan stage.
   - It contains one table: `companies`, and one index: `idx_companies_country`
   - It is ~7MB in size.

These aren't included in the repository because they're large in size. You can
download them by running this script:

```sh
./download_sample_databases.sh
```

If the script doesn't work for some reason, you can download the databases
directly from
[codecrafters-io/sample-sqlite-databases](https://github.com/codecrafters-io/sample-sqlite-databases).
