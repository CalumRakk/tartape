# TarTape

**TarTape** is a Python library that generates standard TAR archives on-the-fly, allowing you to stream multi-gigabyte directories directly to cloud storage without creating temporary archive files on your disk.

---

## The Problem TarTape Solves

Creating and uploading TAR archives with standard tools has common pain points when dealing with large folders (e.g., 5 GB, 20 GB, or 50 GB):

1. **Wasted Disk Space:** Traditional tools build the `.tar` file on disk before uploading. If you have a 30 GB folder, you need another 30 GB of free space just for the temporary archive.
2. **Network Failures Mean Starting Over:** If an upload drops at 90%, you usually have to restart from byte zero because standard TAR streams cannot be safely resumed from an arbitrary byte.
3. **No Way to Upload in Parts:** Cloud providers (like AWS S3 or Azure Blob) work best when uploading in fixed-size parts (e.g., 100 MB or 2 GB), but standard tools produce one monolithic stream.

TarTape solves these issues by turning any directory into a predictable, resumable, and sliceable stream of bytes.

---

## What You Can Do with TarTape

* **Stream directly to the cloud:** Generate TAR bytes in memory as they are sent. Never fill your local disk with temporary archive files.
* **Treat the archive as an open file:** Pass the stream directly to libraries like `requests` or `boto3` as if it were a regular file on disk.
* **Cut the stream into parts:** Divide a multi-gigabyte archive into fixed chunks (e.g., `"500MB"`, `"1GB"`, or `"2GB"`) for cloud multipart uploads. Each chunk behaves like an independent file with its own name and checksum.
* **Upload parts in parallel:** Grab any specific chunk directly (e.g., part #3) to distribute uploads across background workers or threads.
* **Resume from the exact byte:** If a transfer is interrupted, resume it from the exact byte it stopped, without re-reading previous files.
* **Locate and download single files:** Know exactly which part and byte range contains a specific file, allowing you to download only that file from the cloud instead of the entire archive.
* **Predictable output:** The exact same directory produces the exact same byte stream and hash every time (when run on the same operating system).

---

## Installation

```bash
pip install tartape
```

*Requires Python 3.10+.*

---

## Quickstart

Using TarTape is a two-step process:

### Step 1: Index the Directory
First, TarTape performs a quick scan to record file positions and total size. It saves a lightweight index file (`<folder_name>.tartape`) next to your folder without touching its contents:

```python
import tartape

# Scans the folder and creates './my_dataset.tartape' alongside it
tape = tartape.record("./my_dataset")

print(f"Total archive size: {tape.total_size:,} bytes")
print(f"Total files:        {tape.file_count}")
```

### Step 2: Stream the Archive
Open the indexed folder and read it like an ordinary file:

```python
import tartape

with tartape.open("./my_dataset") as tape:
    archive_file = tape.as_file()

    # Read bytes on demand just like an open file
    chunk = archive_file.read(64 * 1024)
```

---

## Practical Examples

### 1. Uploading Directly to Cloud Storage (HTTP / S3)
You don't need intermediate disk storage. Pass the virtual archive directly to your HTTP or cloud client:

```python
import requests
import tartape

with tartape.open("./my_dataset") as tape:
    # 'as_file()' acts like an open file on disk
    response = requests.put(
        "https://storage.example.com/backup.tar", data=tape.as_file()
    )
```

---

### 2. Splitting into Chunks for Cloud Multipart Uploads
Cloud storage providers (like AWS S3) prefer uploads split into parts. TarTape can divide your archive into virtual chunks on-the-fly using human sizes:

```python
import boto3
import tartape

s3 = boto3.client("s3")
bucket = "my-company-backups"

with tartape.open("./my_dataset") as tape:
    # Split the archive into 1 GB virtual chunks ('500MB', '2GB', etc.)
    for volume in tape.iter_volumes(size="1GB"):
        with volume:
            print(f"Uploading part: {volume.name} ({volume.size} bytes)...")

            # Each volume behaves like a separate open file
            s3.upload_fileobj(
                Fileobj=volume, Bucket=bucket, Key=f"backups/{volume.name}"
            )

            # Checksum is calculated automatically during upload
            print(f"Part uploaded - MD5: {volume.md5sum}")
```

---

### 3. Uploading Parts in Parallel (Background Workers)
You don't have to upload chunks sequentially. You can fetch any specific volume directly by its index and upload parts concurrently across multiple threads or workers:

```python
from concurrent.futures import ThreadPoolExecutor
import boto3
import tartape

s3 = boto3.client("s3")
bucket = "my-company-backups"


def upload_single_part(part_index: int):
    # Open the tape and request only the specific volume
    with tartape.open("./my_dataset") as tape:
        volume = tape.get_volume(index=part_index, size="1GB")
        with volume:
            s3.upload_fileobj(
                Fileobj=volume, Bucket=bucket, Key=f"backups/{volume.name}"
            )
            print(f"Finished uploading part #{part_index}")


# Upload parts 0, 1, 2, and 3 in parallel
with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(upload_single_part, range(4))
```

---

### 4. Resuming an Interrupted Upload
If a network transfer fails halfway through, you don't need to restart from the beginning:

```python
import tartape

# The byte offset where your upload dropped
FAILED_AT_BYTE = 524_288_000  # e.g., stopped at 500 MB

with tartape.open("./my_dataset") as tape:
    # Instantly jumps to the requested byte without reading earlier files
    for chunk in tape.play(start_offset=FAILED_AT_BYTE):
        network_socket.send(chunk)
```

---

### 5. Finding and Downloading a Single File (Without Downloading Everything)
If you uploaded a 20 GB backup in several 1 GB parts to cloud storage, you don't need to download all 20 GB to retrieve one file. You can check the index to find the exact part and byte position of that file, and download only those bytes using an HTTP Range request:

```python
import requests
import tartape

# Open the index file (source files don't need to be present)
catalog = tartape.open_catalog("./my_dataset.tartape")

# Locate the file across the volumes automatically
location = catalog.locate("my_dataset/reports/annual_2024.pdf")

print(f"File size: {location.file_size} bytes")

# Download only the specific bytes of this file from cloud storage
for fragment in location.fragments:
    part_url = f"https://storage.example.com/backups/part_{fragment.volume_index}.tar"

    # 'fragment.range_header' is precalculated, avoiding off-by-one HTTP math
    res = requests.get(part_url, headers=fragment.range_header)

    with open("restored_annual_2024.pdf", "ab") as f:
        f.write(res.content)

# Tip: If you defined multiple split schemes (e.g., '500MB' and '1GB'),
# you can specify which one to query: catalog.locate(..., tag_or_size="1GB")
```

---

### 6. Tracking Progress (Progress Bars and Logs)
You can listen to file transitions while streaming to update progress bars or logs:

```python
import tartape
from tartape import TarObserver, TarFileStartEvent, TarFileEndEvent


class ProgressTracker(TarObserver):
    def on_file_start(self, event: TarFileStartEvent):
        print(f"Sending: {event.entry.info.arc_path}")

    def on_file_end(self, event: TarFileEndEvent):
        print(f"Finished: {event.entry.info.arc_path}")


with tartape.open("./my_dataset") as tape:
    for chunk in tape.play(observer=ProgressTracker()):
        network_socket.send(chunk)
```

---

### 7. Excluding Files and Folders
You can ignore temporary files, git directories, or custom patterns when recording:

```python
import tartape

tape = tartape.record(
    directory="./my_dataset",
    exclude=[
        ".git",
        "*.tmp",
        "*.log",
        "node_modules",
        "__pycache__",
    ],
    auto_truncate=True,  # Automatically shorten unusually long path names
    overwrite=True,  # Replace existing index if it already exists
)
```

---

### 8. Checking for File Changes Before Streaming
TarTape ensures that what you stream matches what was originally scanned. You can run a quick check before starting a transfer:

```python
import tartape

with tartape.open("./my_dataset") as tape:
    # Performs a fast spot-check across files
    if not tape.verify():
        print("Warning: Files were modified or deleted after indexing!")

    # Or perform a thorough audit of every single file:
    # tape.verify(deep=True)
```

---

## Important Things to Know

* **Determinism Scope:** Running `record()` on the same folder produces the exact same byte sequence and hash when executed on machines running the **same operating system** (e.g., Linux to Linux).
* **Fail-Fast Safety:** If a file is modified, resized, or deleted while TarTape is streaming, the process stops immediately and raises an error. It will never silently send mismatched or corrupt archives.

---

## API Summary

| Function / Method | What it does |
|:---|:---|
| `tartape.record(path, ...)` | Scans a folder and creates its index file. Returns an active `Tape`. |
| `tartape.open(path)` | Opens an already-indexed folder for streaming. |
| `tartape.open_catalog(path)` | Opens only the index file (no source files needed) to inspect contents or look up file locations. |
| `tape.as_file()` | Returns a standard file-like object with `read()` and `seek()` to stream the archive. |
| `tape.play(start_offset=0)` | Yields raw chunks of bytes directly, with optional byte-accurate resumption. |
| `tape.iter_volumes(size="1GB")` | Yields the archive split into file-like chunks (accepts `"100MB"`, `"1GB"`, `"2GB"`, etc.). |
| `tape.get_volume(index, size="1GB")` | Retrieves a specific volume directly without iterating through previous parts. |
| `tape.verify(deep=False)` | Checks if local files on disk match the recorded index. |
| `catalog.locate(arc_path)` | Returns coordinates and precalculated `range_header` for each file fragment. |

---

## License

MIT License. See [LICENSE](LICENSE) for details.
