# ingestion/writers/jsonl_writer.py

import os
from datetime import datetime, timezone
import json

class JSONLWriter:
    def __init__(self, base_dir: str, prefix: str):
        self.base_dir = base_dir
        self.prefix = prefix
        os.makedirs(base_dir, exist_ok=True)
        self.current_hour = None
        self.file = None
        self.current_file = None

    def _rotate_if_needed(self):
        hour = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        if hour != self.current_hour:
            if self.file:
                self.file.close()
            self.current_hour = hour
            fname = f"{self.prefix}_{hour}.jsonl"
            path = os.path.join(self.base_dir, fname)
            self.current_file = path
            self.file = open(path, "a")

    def write(self, record: dict):
        self._rotate_if_needed()
        self.file.write(json.dumps(record) + "\n")
        self.file.flush()
