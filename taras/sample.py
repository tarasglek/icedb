import duckdb
from icedb.log import S3Client, IceLogIO
from icedb import IceDBv3, CompressionCodec
from datetime import datetime
from time import time

# S3 configuration dictionary
S3_CONFIG = {
    "s3_region": "us-east-1",
    "s3_endpoint": "http://localhost:9900",
    "s3_access_key_id": "user", 
    "s3_secret_access_key": "password",
    "s3_use_ssl": False,
    "s3_url_style": "path"  # can be 'path' or 'vhost'
}

# Bucket-specific S3 config not used by DuckDB
S3_BUCKET_CONFIG = {
    "bucket": "testbucket",
    "prefix": "example",
}

# create an s3 client to talk to minio
s3c = S3Client(
    s3prefix=S3_BUCKET_CONFIG["prefix"],
    s3bucket=S3_BUCKET_CONFIG["bucket"],
    s3region=S3_CONFIG["s3_region"],
    s3endpoint=S3_CONFIG["s3_endpoint"],
    s3accesskey=S3_CONFIG["s3_access_key_id"],
    s3secretkey=S3_CONFIG["s3_secret_access_key"]
)

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
    ['event', 'ts'],
    S3_CONFIG["s3_region"],
    S3_CONFIG["s3_access_key_id"],
    S3_CONFIG["s3_secret_access_key"],
    S3_CONFIG["s3_endpoint"],
    s3c,
    "dan-mbp",
    s3_use_path=S3_CONFIG["s3_url_style"] == "path",
    compression_codec=CompressionCodec.ZSTD
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

# Set DuckDB S3 configuration from the config dictionary
for key, value in S3_CONFIG.items():
    if key == "s3_endpoint":
        # Strip protocol prefix by splitting on :// once
        value = value.split("://", 1)[1]
    ddb.execute(f"SET {key}='{value}'")

# Query alive files
query = ("select user_id, count(*), (properties::JSON)->>'page_name' as page "
         "from read_parquet([{}]) "
         "group by user_id, page "
         "order by count(*) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice.data_s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))
