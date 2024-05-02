# TO DO

Plans to extend the "sqlite clone":

## Basic querying:

- [x] REPL
  - [ ] proper error handling
  - [ ] multi-line statements?
- [ ] output modes (ex: box)
- [ ] multiple filters on WHERE clause
- [ ] SELECT with literals as columns
- [ ] SELECT without FROM
- [ ] proper expr evaluation on SELECT and WHERE clause
  - [ ] general logic and arithmetic
  - [ ] comparison operators other than =
  - [ ] columns and literals on both left and right side of comparisons
- [ ] ORDER BY
- [ ] LIMIT

## Advanced querying

- [ ] Support for multi-key indexes
- [ ] Support for multi-key PKs
- [ ] JOIN
  - [ ] Cartesian Product (CROSS JOIN)
  - [ ] "INNER JOIN" without index access
  - [ ] "INNER JOIN" with index access
  - [ ] "LEFT/RIGHT/FULL JOIN" without index
  - [ ] "LEFT/RIGHT/FULL JOIN" with index

## Beyond...

- [ ] CREATE TABLE without PK
- [ ] CREATE TABLE with integer PK
- [ ] INSERT
- [ ] Handling small tables (leaf b-tree pages)
- [ ] Handling larger tables (interior b-tree pages)
- [ ] DROP TABLE
- [ ] DELETE
- [ ] Free list/pages
- [ ] UPDATE
- [ ] In-memory DB ?
- [ ] Persisting to file (no WAL, rollback, etc)
- [ ] ...