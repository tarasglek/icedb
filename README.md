# IceDB

An in-process parquet merge engine for S3. Inserts and merges powered by Python and DuckDB, querying portable to any lang that can query Postgres, and any DB/lang that can parse parquet.

IceDB enables serverless OLAP/data lake hybrid in-process database framework optimized for event-based data. It combines DuckDB, Parquet files, and S3 storage to fully decouple storage and compute, while only paying for compute during query execution time. It also decouples from both the database and programming language, so you can use IceDB in DuckDB, Pandas (Python), a Golang binary bound to ClickHouse, and more.

_Massive WIP_

See https://blog.danthegoodman.com/icedb-v2

<!-- TOC -->
* [IceDB](#icedb)
  * [Performance test](#performance-test)
  * [KNOWN GOTCHAS](#known-gotchas)
  * [Examples](#examples)
  * [Usage](#usage)
    * [`partitionStrategy`](#partitionstrategy)
    * [`sortOrder`](#sortorder)
    * [`formatRow`](#formatrow)
    * [`unique_row_key`](#uniquerowkey)
  * [Pre-installing extensions](#pre-installing-extensions)
  * [Merging](#merging)
  * [Concurrent merges](#concurrent-merges)
  * [Cleaning Merged Files](#cleaning-merged-files)
  * [Custom Merge Query (ADVANCED USAGE)](#custom-merge-query-advanced-usage)
    * [Handling `_row_id`](#handling-rowid)
      * [Deduplicating Data on Merge](#deduplicating-data-on-merge)
      * [Replacing Data on Merge](#replacing-data-on-merge)
      * [Aggregating Data on Merge](#aggregating-data-on-merge)
  * [Multiple Tables](#multiple-tables)
  * [Meta Store Schema](#meta-store-schema)
<!-- TOC -->

## Performance test

From the test, inserting 2000 times with 2 parts, shows performance against S3 and reading in the state and schema

```
============== insert hundreds ==============
this will take a while...
inserted 200
inserted hundreds in 11.283345699310303
reading in the state
read hundreds in 0.6294591426849365
files 405 logs 202
verify expected results
got 405 alive files
[(406, 'a'), (203, 'b')] in 0.638556957244873
merging it
merged partition cust=test/d=2023-02-11 with 203 files in 1.7919442653656006
read post merge state in 0.5759727954864502
files 406 logs 203
verify expected results
got 203 alive files
[(406, 'a'), (203, 'b')] in 0.5450308322906494
merging many more times to verify
merged partition cust=test/d=2023-06-07 with 200 files in 2.138633966445923
merged partition cust=test/d=2023-06-07 with 3 files in 0.638775110244751
merged partition None with 0 files in 0.5988118648529053
merged partition None with 0 files in 0.6049611568450928
read post merge state in 0.6064021587371826
files 408 logs 205
verify expected results
got 2 alive files
[(406, 'a'), (203, 'b')] in 0.0173952579498291
tombstone clean it
tombstone cleaned 4 cleaned log files, 811 deleted log files, 1012 data files in 4.3332929611206055
read post tombstone clean state in 0.0069119930267333984
verify expected results
got 2 alive files
[(406, 'a'), (203, 'b')] in 0.015745878219604492

============== insert thousands ==============
this will take a while...
inserted 2000
inserted thousands in 107.14211988449097
reading in the state
read thousands in 7.370793104171753
files 4005 logs 2002
verify expected results
[(4006, 'a'), (2003, 'b')] in 6.49034309387207
merging it
breaking on marker count
merged 2000 in 16.016802072525024
read post merge state in 6.011193037033081
files 4006 logs 2003
verify expected results
[(4006, 'a'), (2003, 'b')] in 6.683710098266602
# laptop became unstable around here
```

Some notes:

1. Very impressive state read performance with so many files (remember it has to open each one and accumulate the 
   state!)
2. Merging happens very quick
3. Tombstone cleaning happens super quick as well
4. DuckDB performs surprisingly well with so many files (albeit they are one or two rows each)
5. At hundreds of log files and partitions (where most tables should live at), performance was exceptional
6. Going from hundreds to thousands, performance is nearly perfectly linear, sometimes even super linear (merges)!

Having such a large log files (merged but not tombstone cleaned) is very unrealistic. Chances are worst case you 
have <100 log files and hundreds or low thousands of data files. Otherwise you are either not merging/cleaning 
enough, or your partition scheme is far too granular.

The stability of my laptop struggled when doing the thousands test, so I only showed where I could consistently get to.

## KNOWN GOTCHAS

There is a bug in duckdb right now where `read_parquet` will fire a table macro twice, and show the file twice when listing them, but this doesn't affect actual query results: https://github.com/duckdb/duckdb/issues/7897

In the meantime, consider this when listing files in SQL, and add some caching in your function if needed. See a simple method in ./examples/custom-merge.py

## Examples

- [Simple DuckDB query example](examples/simple.py)
- [Custom Merge Query](examples/custom-merge.py)
- [Query with a Golang binary bound to ClickHouse](examples/clickhouse.md) - Shows the power of both portability of IceDB in both language and query execution.
- [Segment Sink Example](examples/segment-sink.py) - getting events directly from a Customer Data Platform like Segment.com
- [Full production-ready Segment sink API with multiple table support, pre-installed extensions, Dockerfile](https://github.com/danthegoodman1/IceDBSegment)

## Usage

```
pip install git+https://github.com/danthegoodman1/icedb
```

```python
from icedb import IceDB

ice = IceDB(...)
```

### `partitionStrategy`

Function that takes in an `dict`, and returns a `str`. How the partition is determined from the row dict.

Example:

```python
from datetime import datetime

def partStrat(row: dict) -> str:
    rowTime = datetime.utcfromtimestamp(row['timestamp']/1000) # timestamp is in ms
    return 'y={}/m={}/d={}'.format('{}'.format(rowTime.year).zfill(4), '{}'.format(rowTime.month).zfill(2), '{}'.format(rowTime.day).zfill(2))

ice = IceDB(partitionStrategy=partStrat, sortOrder=['event', 'timestamp'])
```

### `sortOrder`

Defines the order of top-level keys in the row dict that will be used for sorting inside of the parquet file.

Example:

```python
['event', 'timestamp']
```

### `formatRow`

Format row is the function that will determine how a row is finally formatted to be inserted. This is where you would want to flatten JSON so that the data is not corrupted. This is called just before inserting into a parquet file.

**It's crucial that you flatten the row and do not have nested objects**

Example:

```python
def format_row(row: dict) -> dict:
    row['properties'] = json.dumps(row['properties']) # convert nested dict to json string
    return row
```

### `unique_row_key`

If provided, will use a top-level row key as the `_row_id` for deduplication. If not provided a UUID will be generated.

## Pre-installing extensions

DuckDB uses the `httpfs` extension. See how to pre-install it into your runtime here: https://duckdb.org/docs/extensions/overview.html#downloading-extensions-directly-from-s3

and see the `extension_directory` setting: https://duckdb.org/docs/sql/configuration.html#:~:text=PHYSICAL_ONLY-,extension_directory,-Set%20the%20directory with the default of `$HOME/.duckdb/`

## Merging

Merging takes a `max_file_size`. This is the max file size that is considered for merging, as well as a threshold for when merging will start. This means that the actual final merged file size (by logic) is in theory 2*max_file_size, however due to parquet compression it hardly ever gets that close.

For example if a max size is 10MB, and during a merge we have a 9MB file, then come across another 9MB file, then the threshold of 10MB is exceeded (18MB total) and those files will be merged. However with compression that final file might be only 12MB in size.

## Concurrent merges

Concurrent merges won't break anything due to the isolation level employed in the meta store transactions, however there is a chance that competing merges can result in conflicts, and when one is detected the conflicting merge will exit. Instead, you can choose to immediately call `merge` again (or with a short, like 5 seconds) if you successfully merged files to ensure that lock contention stays low.

However concurrent merges in opposite directions is highly suggested.

For example in the use case where a partition might look like `y=YYYY/m=MM/d=DD` then you should merge in `DESC` order frequently (say once every 15 seconds). This will keep the hot partitions more optimized so that queries on current data don't get too slow. These should have smaller file count and size requirements so they can be fast, and reduce the lock time of files in the meta store.

You should run a second, slower merge internal in `ASC` order that fully optimizes older partitions. These merges can be much large in file size and count, as they are less likely to conflict with active queries. Say this is run every 5 or 10 minutes.

## Cleaning Merged Files

Using the `remove_inactive_parts` method, you can delete files with some minimum age that are no longer active. This helps keep S3 storage down.

For example, you might run this every 10 minutes to delete files that were marked inactive at least 2 hours ago.

## Custom Merge Query (ADVANCED USAGE)

You can optionally provide a custom merge query to achieve functionality such as aggregate-on-merge or replace-on-merge as found in the variety of ClickHouse engine tables such as the AggregatingMergeTree and ReplacingMergeTree.

This can also be used along side double-writing (to different partition prefixes) to create materialized views!

**WARNING: If you do not retain your merged files, bugs in merges can permanently corrupt data. Only customize merges if you know exactly what you are doing!**

This is achieved through the `custom_merge_query` function. You should not provide any parameters to this query.

The default query is:

```sql
select *
from source_files
```

The `?` **must be included**, and is the list of files being merged.

`source_files` is just an alias for `read_parquet(?, hive_partitioning=1)`, which will be string-replaced if it exists. Note that the `hive_partitioning` columns are virtual, and do not appear in the merged parquet file, therefore is it not needed.

### Handling `_row_id`

#### Deduplicating Data on Merge

By default, no action is taken on `_row_id`. However you can use this to deduplicate in both analytical queries, and custom merge queries.

For example, if you wanted merges to take any (but only a single) value for a given `_row_id`, you might use:

```sql
select
    any_value(user_id),
    any_value(properties),
    any_value(timestamp),
    _row_id
from source_files
group by _row_id
```

Note that this will only deduplicate for a single merged parquet file, to guarantee single rows you much still employ deduplication in your analytical queries.

#### Replacing Data on Merge

If you wanted to replace rows with the most recent version, you could write a custom merge query that looks like:

```sql
select
    argMax(user_id, timestamp),
    argMax(properties, timestamp),
    max(timestamp),
    _row_id
from source_files
group by _row_id
```

Like deduplication, you must handle this in your queries too if you want to guarantee getting the single latest row.

#### Aggregating Data on Merge

If you are aggregating, you must include a new `_row_id`. If you are replacing this should come through choosing the correct row to replace.

Example aggregation merge query:

```sql
select
    user_id,
    sum(clicks) as clicks,
    gen_random_uuid()::TEXT as _row_id
from source_files
group by user_id
```

This data set will reduce the number of rows over time by aggregating them by `user_id`.

**Pro-Tip: Handling Counts**

Counting is a bit trickier because you would normally have to pivot from `count()` when merging a never-before-merged file to `sum()` with files that have been merged at least once to account for the new number. The trick to this is instead adding a `counter` column with value `1` every time you insert a new row.

Then, when merging, you simply `sum(counter) as counter` to keep a count of the number of rows that match a condition.

## Multiple Tables

Multiple tables can be achieved through a combination of the `partition_prefix` parameter in `IceDB.merge_files`, as well as partitioning via a Hive partitioning key such as:

```
table={}/y={}/m={}/d={}
```

You can then easily parameterize partition ranges like:

```python
def get_partition_range(table: str, syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    return ['table={}/y={}/m={}/d={}'.format(table, '{}'.format(syear).zfill(4), '{}'.format(smonth).zfill(2), '{}'.format(sday).zfill(2)),
            'table={}/y={}/m={}/d={}'.format(table, '{}'.format(eyear).zfill(4), '{}'.format(emonth).zfill(2), '{}'.format(eday).zfill(2))]
```

## Meta Store Schema

The meta store uses the following DB table:
```sql
create table if not exists known_files (
    partition TEXT NOT NULL,
    filename TEXT NOT NULL,
    filesize INT8 NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    _created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(active, partition, filename)
)
```

This means that you can simply query this table from other languages as shown in the [Golang example](ch/user_scripts/main.go), and [query it from other databases like ClickHouse](examples/clickhouse.md)
