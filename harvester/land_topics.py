"""Land the OpenAlex `topics` snapshot entity -- the one place this project
reads the actual Parquet snapshot rather than the REST API.

Every other entity here is API-sourced: `works` because a partition slice's
citation graph is too sparse (see snowball.py), `authors`/institutions
because they're derived straight from each harvested work's own `authorships`
field, no separate fetch needed. `topics` is different: it's OpenAlex's
curated ~4,500-row taxonomy (domain -> field -> subfield -> topic), small
enough to land whole, and useful for a full-taxonomy browse/rollup view (gold
can then show "0 papers yet" topics, not just topics that happen to appear in
the harvested corpus). Landing it from the real snapshot -- not just calling
GET /topics -- is deliberate: it's the project's genuine bulk-Parquet-over-
HTTPS touchpoint, proven to work unauthenticated during corpus-strategy
planning (see the plan's "What I verified" section).

Run standalone: python harvester/land_topics.py --out ./harvest/topics.jsonl
"""

from __future__ import annotations

import argparse
import io
import json
import os
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

MANIFEST_URL = "https://openalex.s3.amazonaws.com/data/parquet/manifest.json"
ENTITY = "topics"
COLUMNS = ["id", "display_name", "domain", "field", "subfield", "description", "keywords"]


class _SeekableHttpFile(io.RawIOBase):
    """Minimal seekable file over HTTP range requests -- pyarrow's Parquet
    reader needs `readinto`, not just `read`, and needs the file to be
    seekable (the footer is at the end). Same shape proven during corpus
    density probing; kept separate rather than shared to avoid the harvester
    module depending on pyarrow, which it otherwise has no need for.
    """

    def __init__(self, url: str):
        self.url = url
        self.pos = 0
        head = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(head, timeout=60) as resp:
            self.size = int(resp.headers["Content-Length"])

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.pos

    def seek(self, offset: int, whence: int = 0) -> int:
        self.pos = offset if whence == 0 else (self.pos + offset if whence == 1 else self.size + offset)
        return self.pos

    def readinto(self, b) -> int:  # noqa: ANN001
        n = len(b)
        if self.pos >= self.size or n == 0:
            return 0
        end = min(self.pos + n - 1, self.size - 1)
        req = urllib.request.Request(self.url, headers={"Range": f"bytes={self.pos}-{end}"})
        with urllib.request.urlopen(req, timeout=180) as resp:
            data = resp.read()
        b[: len(data)] = data
        self.pos += len(data)
        return len(data)


def list_partition_files() -> list[str]:
    """OpenAlex publishes one combined manifest for every parquet entity
    (data/parquet/manifest.json: {entities: [{entity, files: [{url, meta}]}, ...]}),
    not a separate per-entity manifest file -- confirmed by browsing the
    actual bucket structure directly after a 404 against the older
    per-entity path this originally targeted. `files` for the topics entity
    is a handful of small updated_date partitions (~4,500 rows total, not
    one big file), same as every other entity here.
    """
    req = urllib.request.Request(MANIFEST_URL)
    with urllib.request.urlopen(req, timeout=60) as resp:
        manifest = json.load(resp)
    entity = next(e for e in manifest["entities"] if e["entity"] == ENTITY)
    return [f["url"].replace("s3://openalex/", "https://openalex.s3.amazonaws.com/") for f in entity["files"]]


def read_partition(url: str) -> list[dict]:
    src = pa.PythonFile(io.BufferedReader(_SeekableHttpFile(url), buffer_size=1 << 20), mode="r")
    pf = pq.ParquetFile(src)
    rows = []
    for g in range(pf.metadata.num_row_groups):
        rows.extend(pf.read_row_group(g, columns=COLUMNS).to_pylist())
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=os.environ.get("OPENALEX_TOPICS_OUT", "./harvest/topics.jsonl"))
    args = parser.parse_args()

    files = list_partition_files()
    print(f"topics snapshot: {len(files)} partition file(s)")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with out_path.open("w", encoding="utf-8") as f:
        for url in files:
            rows = read_partition(url)
            for row in rows:
                f.write(json.dumps(row) + "\n")
            total += len(rows)
            print(f"  {url.rsplit('/', 1)[-1]}: {len(rows):,} rows")

    print(f"wrote {total:,} topics to {out_path}")


if __name__ == "__main__":
    main()
