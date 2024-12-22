import duckdb
from icedb.log import S3Client, IceLogIO
from icedb import IceDBv3, CompressionCodec
from datetime import datetime
from time import time

# create an s3 client to talk to minio
s3c = S3Client(s3prefix="example", s3bucket="testbucket", s3region="us-east-1", s3endpoint="http://localhost:9000",
               s3accesskey="user", s3secretkey="password")

example_events = [
    {
        "ts": 1686176939445,
        "event": "page_load",
        "user_id": "user_a",
        "properties": {
            "page_name": "Home"
        }
    }, {
        "ts": 1676126229999,
        "event": "page_load",
        "user_id": "user_b",
        "properties": {
            "page_name": "Home"
        }
    }, {
        "ts": 1686176939666,
        "event": "page_load",
        "user_id": "user_a",
        "properties": {
            "page_name": "Settings"
        }
    }, {
        "ts": 1686176941445,
        "event": "page_load",
        "user_id": "user_a",
        "properties": {
            "page_name": "Home"
        }
    }
]


def part_func(row: dict) -> str:
    """
    Partition by user_id, date
    """
    row_time = datetime.utcfromtimestamp(row['ts'] / 1000)
    part = f"u={row['user_id']}/d={row_time.strftime('%Y-%m-%d')}"
    return part


# Initialize the client
ice = IceDBv3(
    part_func,
    ['event', 'ts'],  # Sort by event, then timestamp of the event within the data part
    "us-east-1",
    "user",
    "password",
    "http://localhost:9000",
    s3c,
    "dan-mbp",
    s3_use_path=True,  # needed for local minio
    compression_codec=CompressionCodec.ZSTD  # Let's force a higher compression level, default is SNAPPY
)

# Insert records
inserted = ice.insert(example_events)
print('inserted', inserted)

# Read the log state
log = IceLogIO("demo-host")
_, file_markers, _, _ = log.read_at_max_time(s3c, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, file_markers))

# Setup duckdb for querying local minio
ddb = duckdb.connect(":memory:")
ddb.execute("install httpfs")
ddb.execute("load httpfs")
ddb.execute("SET s3_region='us-east-1'")
ddb.execute("SET s3_access_key_id='user'")
ddb.execute("SET s3_secret_access_key='password'")
ddb.execute("SET s3_endpoint='localhost:9000'")
ddb.execute("SET s3_use_ssl='false'")
ddb.execute("SET s3_url_style='path'")

# Query alive files
query = ("select user_id, count(*), (properties::JSON)->>'page_name' as page "
         "from read_parquet([{}]) "
         "group by user_id, page "
         "order by count(*) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice.data_s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))
