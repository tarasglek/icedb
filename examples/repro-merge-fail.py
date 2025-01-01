from time import time, sleep
from helpers import delete_all_s3
import duckdb
from icedb.log import S3Client, IceLogIO, FileMarker
from icedb import IceDBv3, CompressionCodec
from datetime import datetime

# S3 configuration dictionary
S3_CONFIG = {
    "s3_region": "us-east-1",
    "s3_endpoint": "http://localhost:9000",
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

# wipe everything at the start
delete_all_s3(s3c)

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
    partition_function=part_func,  # Partitions by user_id and date
    sort_order=['event', 'ts'],   # Sort by event, then timestamp of the event within the data part
    # S3 settings from config
    s3_region=S3_CONFIG["s3_region"],
    s3_access_key=S3_CONFIG["s3_access_key_id"],
    s3_secret_key=S3_CONFIG["s3_secret_access_key"],
    s3_endpoint=S3_CONFIG["s3_endpoint"],
    s3_use_path=S3_CONFIG["s3_url_style"] == "path",
    # S3 client instance
    s3_client=s3c,
    # Other settings
    path_safe_hostname="dan-mbp",
    compression_codec=CompressionCodec.ZSTD,  # Use ZSTD for higher compression ratio compared to default SNAPPY
)

def once():

    def print_file_stats(description: str, files: list[FileMarker]):
        """Print summary and details of file markers.
        
        Args:
            description: Description of the file group (e.g. "alive", "tombstoned")
            files: List of FileMarker objects to summarize
        """
        paths = list(map(lambda x: x.path, files))
        print(f"{len(paths)} {description} files:", paths)


    # Insert records
    inserted = ice.insert(example_events)
    print_file_stats("inserted", inserted)

    # Read the log state
    log = IceLogIO("demo-host")
    _, file_markers, _, _ = log.read_at_max_time(s3c, round(time() * 1000))
    alive_files = list(filter(lambda x: x.tombstone is None, file_markers))
    tombstoned_files = list(filter(lambda x: x.tombstone, file_markers))
    print_file_stats("alive", alive_files)
    print_file_stats("tombstoned", tombstoned_files)

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

    while True:
        new_log, new_file_marker, partition, merged_file_markers, meta = ice.merge()
        more_to_merge = partition is not None
        if partition:  # if any merge happened
            print(f"Merged partition: {partition}")
            if merged_file_markers:
                print_file_stats("source files merged", merged_file_markers)
            if new_file_marker:
                print_file_stats("new merged file", [new_file_marker])
        else:
            break;
    tombstoned = ice.tombstone_cleanup(10_000)
    cleaned_logs, deleted_logs, deleted_data = tombstoned
    print(f"{len(cleaned_logs)} cleaned log files:", cleaned_logs)
    print(f"{len(deleted_logs)} deleted log files:", deleted_logs)
    print(f"{len(deleted_data)} deleted data files:", deleted_data)


for i in range(30):
    try:
        once()
    except Exception as e:
        print(f"Failed after {i} runs")
        raise e
    sleep(1)