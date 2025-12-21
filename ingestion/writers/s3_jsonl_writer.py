import json
import boto3
from datetime import datetime
from collections import defaultdict
import uuid
import time


class S3JSONLWriter:
    def __init__(self, bucket: str, prefix: str = "market-bronze"):
        self.bucket = bucket
        self.prefix = prefix
        self.s3 = boto3.client("s3")
        self.buffers = defaultdict(list)
        self.last_flush = {}
        self.flush_interval_seconds = 30


    def _partition_key(self, record):
        ts = datetime.fromisoformat(record["event_ts"].replace("Z", "+00:00"))


        
        return (
            f"exchange={record['exchange']}/"
            f"symbol={record['symbol'].replace('/', '-')}/"
            f"interval_minutes={record['interval_minutes']}/"
            f"year={ts.year}/"
            f"month={ts.month:02d}/"
            f"day={ts.day:02d}/"
            f"hour={ts:%H}/"
        )

    def write(self, record):
        key_prefix = self._partition_key(record)
        now = time.time()

        self.buffers[key_prefix].append(json.dumps(record))

        last = self.last_flush.get(key_prefix, 0)

        if (
            len(self.buffers[key_prefix]) >= 500
            or now - last >= self.flush_interval_seconds
        ):
            self.flush(key_prefix)
            self.last_flush[key_prefix] = now


    def flush(self, key_prefix):
        if not self.buffers[key_prefix]:
            return

        key = f"{key_prefix}part-{uuid.uuid4().hex}.jsonl"
        body = "\n".join(self.buffers[key_prefix]) + "\n"

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode("utf-8"),
        )

        self.buffers[key_prefix].clear()

    def flush_all(self):
        for key_prefix in list(self.buffers.keys()):
            self.flush(key_prefix)
