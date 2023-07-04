# ThomasSQL

This repo is the completed version of the ClassProject of 2023Spring CSCI485.

## Overview
This project builds a relational databse based on [FoundationDB](https://www.foundationdb.org/) - a NoSQL databse.
The database implemented following functions:
- Create, drop, edit tables with primary keys.
- Insert, update, delete columns.
- Cursors that can iterate over a table in forward or backward order.
- Hash indexing and B-tree indexing.
- Relation algebra such as join, group ...
- Transactions with ACID properties.

## Project Structure Overview

- `lib`: the dependencies of the project
- `src`: the source codes of the project
  - `CSCI485ClassProject`: root package of the project
    - `models`: package storing some defined representations of models in the database.
    - `test`: package for unit tests
    
## Run Tests on macOS/Linux using `make`

If you are developing in `macOS/Linux` environment(recommended), we provide `Makefile` for you to run tests quickly.

To run tests of partX, use command
```shell
make partXTest
```

As you may have different project structures, Makefile may not work in your implementation. In this case, you can change the `sources` variable in Makefile by adding the name of the java files you created to it.
Note that the order of the file should align with the class dependency relationship, i.e. class `A` imports `B`, then `B.java` should be in front of `A.java` in `sources`.
