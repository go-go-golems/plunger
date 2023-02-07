# DB Layout

The goal of plunger is to make it easy to store structured logs in a SQLite
DB in such a way that it is easy to query it and get value out of it.

## Logging sessions

## Denormalized tables for specific structures

Because structured log entries can be arbitrary, and these can be hard to query
(if for example just stored as a simple log entry line and a key/value table),
it makes sense to flatten out certain log-entries into more normalized tables.

The idea for plunger is to pass in a "type" to the structured logged, 
which can then be used to select a separate table to insert the log into, only leaving 
the "type" and "timestamp" in the main log table (and potentially non-covered fields into the
normal default key/value table).

## Scaffold query builders for selecting 

In order to make accessing these tables as simple as possible, we want to store the
denormalization structure itself into the DB.
