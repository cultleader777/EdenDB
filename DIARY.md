* 2022-06-14

Stuff works.

I check that:
1. Every table has maximum of 1 primary key
2. Every table names/column names are lowercase
3. All column names are unique

I need to check more context for more errors.

1. I can check types once all source files are parsed
2. Typed columns, which are basically string but validated and still enforced (like ipv4/ipv6)
3. DONE Parse unique constraints on a table

* 2022-06-19

I have sources parsed as data!

Let's starting testing out them

* 2022-06-20

Muh pattern framework is coming along, parser is separated from checker.

DONE: start inserting data
DONE: implement default values
  - DONE parser
  - DONE constraint that default values cannot be foreign keys

* 2022-06-21

DAYUM, data insertion works!
DONE: check foreign keys in other tables per table data vector
DONE: check that primary keys are unique per table data vector
DONE: insert with inner values
DONE: check unique constraints per table
DONE: after insertion check that all vectors in the table have the same length
DONE: check that table with referred to by foreign key has primary key, and physical foreign key value matches destination table value
DONE: float cannot be foreign/primary key
DONE: make validation error fields explicit in what they mean
DONE: prevent recursive insert to table with WITH statement
DONE: extra table not found check
DONE: check that extra table has one unambigous reference to this table
DONE: check for multiple, possibly ambigous references to this table
DONE: with statement cannot refer to a column that is used as primary key to the parent table
DONE: refactor split validation regions

* 2022-06-22

A lot of done, with works, we can start analyzing the data finally?

Also, override defaults statements and tracking which columns were overriden

DONE: float columns cannot be primary keys
DONE: float columns cannot be in unique constraints
TODO: with can be done recursively likely, if with tables themselves have primary columns

* 2022-06-26

TODO: we don't return syntax errors when parsing
TODO: syntax errors are ignored now

* 2022-06-27

Uniq constraint done. We should catch syntax errors!

DONE: explicitly specify REF target if ambigous
 ^ not needed, if people want to be explicit they can define raw tuples without WITH statement

TODO: the children!!
  1. inherit primary keys from upper layers
  2. ensure there are no cycles in inheritance
  3. assert there are no duplicate keys by multiple columns, like unique constraint

* 2022-06-28

DONE: ensure inherited column names don't clash with existing column names

* 2022-07-04

DONE: parse layered WITH WITH statements
DONE: test json output data
DONE: refactor insert extra data function to recursively process everything

* 2022-07-06

TODO: more tests for recursive stuff
TODO: enums? OnlyCapitalVariants
TODO: data tuples insert mode
DONE: remove assert_compiles test and replace with assert_compiles_data

* 2022-07-07

DONE: mix child pkeys with foreign keys, child pkeys are preferred
DONE: explicit fields father keys

* 2022-07-08

DONE: split up sources
DONE: enums are just tables with single column and everything will be checked already
TODO: with json like tuples definition
TODO: sqlite engine
DONE: constrain only single DATA EXCLUSIVE entry, like enum, multiple are forbidden

* 2022-07-09

TODO: forbid child tables to be foreign keys, maybe in future?
DONE: parse nested fields, now just crunch that data

* 2022-07-12

DONE: Default value refactor.

How to ergonomically insert rows to columns?

Have single format of uniform dataframes which throws errors, like insert data stuff and use that on both ends?

DONE: quite a few refactorings
TODO: split up extra data insertion function.

* 2022-07-13

DONE: fix recursion bullshit
1. we need to compute next stack value for insertion of extra data
2. We need insertion mode to know what to pick
3. If it is foreign key we append primary key downstream
4. If it is child key we append current child downstream
5. We certainly should have kv stack with all fields to append downstream
6. If table structure is unspecified, do default tuple order and throw away everything not contained
7. Good luck

* 2022-07-17

DONE: fixed tests, recursion should werk
TODO: forbid column names like INT ir TEXT that would clash with sqlite
DONE: smashed bugs of simple insert for structured
DONE: failing test

* 2022-07-18

Fixed regressions.
Now, keep going for coverage until I'm reasonable sure all is good?
DONE: Lua checks columns
DONE: more regressions with implicit column order
DONE: yet another deeper nested regression found

* 2022-07-19

DONE: Lua checks implemented
DONE: Fix workaround of appending return keyword
DONE: Lua load inline sources into runtime
DONE: Lua computed columns
DONE: Inefficiency in lua eval, we add and reset every rows values for every expression, 3x work for three expressions
TODO: Lua load from source file

* 2022-07-20

DONE: lua computed columns with tests, fully 100% completed in one day!

* 2022-07-21

DONE: test we cannot mutate database
DONE: test some runtime error
DONE: implemented sqlite proofs engine in one day!
DONE: prolog proof engine?

* 2022-07-22

DONE: implement booleans?.. cannot be primary keys
DONE: prohibited NaN and infinity floats

* 2022-07-24

DONE: sqlite materialized views

* 2022-07-25

DONE: test the table rule scenarios
DONE: finish the success testcase, we're very close

* 2022-07-26

Implemented booleans
Implemented sqlite materialized views

TODO: rust generator of DB
DONE: include model for multiple files

* 2022-07-27

DONE: cli reader command line app with circular dependencies and stuff
DONE: test cli executable itself

Now, just generate rust sources

DONE: generate column row ids for child/parent stuff and foreign keys
Very productive day, most of db implementation and API done!

After figuring out foreign keys we can do serialization/deserialization

* 2022-07-28

DONE: built index of foreign key relationships with vectors
DONE: build index of parent key rows
DONE: fix the regression test
DONE: the rust gen stuff!

DONE: add binary xxhash + lz4 compression

* 2022-07-29

DONE: rust output dump.
DONE: pretty test assertions
DONE: lz4 compression
DONE: write files only if changed
DONE: xxhash, after all checksum is only in frame mode

DONE: ocaml codegen

* 2022-07-30

DONE: output binary data assumption test

* 2022-07-31

DONE: ocaml deserialization function
DONE: fix loading ppx_blob issue
DONE: write one ocaml dump test

TODO: ocaml, figure out checksums + hashing for edb data

* 2022-08-02

DONE: materialized views outputs are sorted and deterministic

* 2022-08-07

DONE: foreign keys for child tables work
DONE: added foreign keys to child elements index
DONE: add index of child elements to foreign key elements
DONE: binary test for child elements index
DONE: fix regressions

* 2022-08-10

DONE: implement relationships of foreign keys showing all its referrers

Now, let's fix the regressions... And...

Mustache based template language for snippet generation?

* 2022-08-14

DONE: lua data generation
DONE: exclusive lock for data in lua

* 2022-08-16

TODO: compute ref count method for all referrees. We don't need ref count separate methods, because we can get children/referrees
TODO: sqlite dump
TODO: json dump

* 2022-08-17

DONE: find common ancestor for keys, not just first ref parent

* 2022-08-18

DONE: pin directory for context to source reader.
If file next to current source file exists, use that.

* 2022-10-06

DONE: fix remaining tests
DONE: success tests

* 2022-10-26

DONE: single quotes same as double quotes

* 2022-11-19

DONE: REF FOREIGN CHILD and REF CHILD.

Foreign child is when referring to the foreign child by the parent.
REF CHILD is when referring to your own child.

* 2023-05-27

DONE: implement detached defaults

Mark column as DETACHED DEFAULT in the table which must be provided as separate in the source

Every detached default must be defined only once.

Three errors added:
1. Detached default not provided
2. Detached default is wrong type
3. Detached default defined more than once

```
TABLE server {
  hostname TEXT PRIMARY KEY,
  tld REF tld DETACHED DEFAULT,
  something_else INT DETACHED DEFAULT,
}

DEFAULTS {
    server.tld "epl-infra.net",
    server.something_else 777,
}
```
