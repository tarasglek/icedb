"""
Shows how you can achieve functionality like ClickHouse's AggregatingMergeTree engine.
This example keeps track of a running count of events per user.

Counting is just a sum of occurrences, so we simply can create a custom merge query that introduces a new
column called `cnt` when merged. We then add that column during ingestion with a default of `1`.
Then we sum that to get the count of rows, constantly reducing the number of rows when merged.

It's important that we apply the same sum to query the data as
merging does not guarantee reducing to a single row, as data across parts may not fully merge.
For end-user querying you could always replace a fake table state-finisher with this as a subquery.

Run:
`docker compose up -d`

Then:
`python materialized-view.py`
"""

from icedb.icedb import IceDBv3, CompressionCodec
from icedb.log import IceLogIO
from datetime import datetime
import json
from time import time
from helpers import get_local_ddb, get_local_s3_client, delete_all_s3, get_ice

s3c_raw = get_local_s3_client(prefix="example_raw")
s3c_mv = get_local_s3_client(prefix="example_mv")


def part_func_raw(row: dict) -> str:
    """
    We'll partition by user_id, date
    """
    row_time = datetime.utcfromtimestamp(row['ts'] / 1000)
    part = f"u={row['user_id']}/d={row_time.strftime('%Y-%m-%d')}"
    return part


def part_func_mv(row: dict) -> str:
    """
    We'll partition by user_id only here
    """
    row_time = datetime.utcfromtimestamp(row['ts'] / 1000)
    part = f"u={row['user_id']}"
    return part


def format_row_raw(row: dict) -> dict:
    """
    We can take the row as-is, except let's make the properties a JSON string for safety
    """
    row['properties'] = json.dumps(row['properties'])  # convert nested dict to json string
    return row


def format_row_mv(row: dict) -> dict:
    """
    Considering this would be a materialized view on raw incoming data,
    we prepare it differently than `simple.py`
    """
    del row['properties'] # drop properties because we don't need it for this table
    row['cnt'] = 1 # seed the incoming columns with the count to sum
    return row


# This will be for the raw events
ice_raw = get_ice(s3c_raw, part_func_raw, format_row_raw)
# This will be for our materialized view
ice_mv = get_ice(s3c_mv, part_func_mv, format_row_mv)

# Some fake events that we are ingesting, pretending we are inserting a second time into a materialized view
example_events = [
    {
        "ts": 1686176939445,
        "event": "page_load",
        "user_id": "user_a",
        "properties": {
            "page_name": "Home"
        },
    }, {
        "ts": 1676126229999,
        "event": "page_load",
        "user_id": "user_b",
        "properties": {
            "page_name": "Home"
        },
    }, {
        "ts": 1686176939666,
        "event": "page_load",
        "user_id": "user_a",
        "properties": {
            "page_name": "Settings"
        },
    }, {
        "ts": 1686176941445,
        "event": "page_load",
        "user_id": "user_a",
        "properties": {
            "page_name": "Home"
        },
    }
]

print("============= inserting events (into both icedb instances) ==================")
inserted = ice_raw.insert(example_events)
firstInserted = list(map(lambda x: x.path, inserted))
print('inserted raw', firstInserted)

inserted = ice_mv.insert(example_events)
firstInserted = list(map(lambda x: x.path, inserted))
print('inserted mv', firstInserted)

# Read the state in
log = IceLogIO("dan-mbp")
s1, f1, t1, l1 = log.read_at_max_time(s3c_raw, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))

print("============= check number of raw rows in data =============")
# Create a duckdb instance for querying
ddb = get_local_ddb()

# Run the query
s1, f1, t1, l1 = log.read_at_max_time(s3c_raw, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))
query = ("select user_id, event, count(*) as cnt "
         "from read_parquet([{}]) "
         "group by user_id, event, event "
         "order by count(user_id) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice_raw.s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

print("============= perform query that shows the running count =============")

# Run the query
s1, f1, t1, l1 = log.read_at_max_time(s3c_mv, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))
query = ("select user_id, event, sum(cnt) as cnt "
         "from read_parquet([{}]) "
         "group by user_id, event "
         "order by sum(cnt) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice_mv.s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

print("============= inserting more events ==================")
inserted = ice_raw.insert(example_events)
firstInserted = list(map(lambda x: x.path, inserted))
print('inserted raw', firstInserted)
inserted = ice_mv.insert(example_events)
firstInserted = list(map(lambda x: x.path, inserted))
print('inserted mv', firstInserted)

print("============= check number of raw rows in data =============")

# Run the query
s1, f1, t1, l1 = log.read_at_max_time(s3c_raw, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))
query = ("select user_id, count(*) as cnt "
         "from read_parquet([{}]) "
         "group by user_id, event "
         "order by count(user_id) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice_raw.s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

print("============= perform query that shows the running count =============")

# Run the query
s1, f1, t1, l1 = log.read_at_max_time(s3c_mv, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))
query = ("select user_id, event, sum(cnt) as cnt "
         "from read_parquet([{}]) "
         "group by user_id, event "
         "order by sum(cnt) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice_mv.s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

print("============= merging =============")
# merge twice to hit both partitions
merged_log, new_file, partition, merged_files, meta = ice_raw.merge()
print(f"raw table merged {len(merged_files)} data files from partition {partition}")
merged_log, new_file, partition, merged_files, meta = ice_raw.merge()
print(f"raw table merged {len(merged_files)} data files from partition {partition}")

# here we reduce the rows to a single sum, and we'll keep the
# latest ts to know when it last updated, we merge twice to hit both partitions
merged_log, new_file, partition, merged_files, meta = ice_mv.merge(custom_merge_query="""
select sum(cnt)::INT8 as cnt, max(ts) as ts, user_id, event
from source_files
group by user_id, event
""")
print(f"mv table merged {len(merged_files)} data files from partition {partition}")
merged_log, new_file, partition, merged_files, meta = ice_mv.merge(custom_merge_query="""
select sum(cnt)::INT8 as cnt, max(ts) as ts, user_id, event
from source_files
group by user_id, event
""")
print(f"mv table merged {len(merged_files)} data files from partition {partition}")

print("============= check number of rows in raw table =============")
print("(we merged the MV, so this didn't change)")

# Run the query
s1, f1, t1, l1 = log.read_at_max_time(s3c_raw, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))
query = ("select user_id, count(*) as cnt "
         "from read_parquet([{}]) "
         "group by user_id, event "
         "order by count(user_id) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice_raw.s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

print("============= check number of rows in the materialized view =============")
print("(we merged the MV, so this didn't change)")

# Run the query
s1, f1, t1, l1 = log.read_at_max_time(s3c_mv, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))
query = ("select user_id, count(*) as cnt "
         "from read_parquet([{}]) "
         "group by user_id, event "
         "order by count(user_id) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice_mv.s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

print("============= perform query that shows the running count =============")
print("(but we still maintained the running count!)")

# Run the query
s1, f1, t1, l1 = log.read_at_max_time(s3c_mv, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))
query = ("select user_id, event, sum(cnt)::INT8 as cnt "
         "from read_parquet([{}]) "
         "group by user_id, event "
         "order by sum(cnt) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice_mv.s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

delete_all_s3(s3c_raw)
delete_all_s3(s3c_mv)
