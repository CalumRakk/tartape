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

## The Mental Model: Catalog, Tape, and Volumes

To get the most out of TarTape, it helps to understand its three core concepts:

* **Catalog (`.tartape`):** The **Blueprint & Map**. A lightweight sidecar database containing the deterministic file inventory, byte offsets, and cryptographic signatures. It operates in **Zero-Disk mode**—it can be inspected, queried, and verified on any machine without needing the original files.
* **Tape:** The **Streaming Engine**. Tied to your local directory on disk and its catalog. It materializes the archive on-the-fly as a continuous, resumable virtual byte stream without intermediate disk files.
* **Volume:** The **Independent Slice**. A deterministic window of the tape (e.g., divided into 1 GB chunks) designed for cloud multipart uploads. Each volume behaves like a separate open file with its own name and non-blocking in-flight checksum.

```text
               Your Directory on Disk (e.g., 5 GB)
               └── dataset/
                     │
                     ▼ tartape.record()
       ┌──────────────────────────────────────────────────┐
       │ CATALOG (dataset.tartape)                        │
       │ Zero-Disk sidecar index: file inventory,         │
       │ global offsets, hashes, and slicing schemes.     │
       └───────────────┬──────────────────┬───────────────┘
                       │                  │
        STREAMING PATH │                  │ OFFLINE GPS PATH
        (Needs source  │                  │ (Index only, zero disk)
         files on disk)│                  │
                       ▼                  ▼
        ┌─────────────────────────┐  ┌─────────────────────────┐
        │ TAPE: Continuous Stream │  │ catalog.locate(file)    │
        │ [Header][Data][Padding] │  │ Coordinates & HTTP      │
        └──────────────┬──────────┘  │ Range headers for       │
                       │             │ surgical downloads      │
        tape.iter_volumes("2GB")     └─────────────────────────┘
                       │
       ┌───────────────┴───────────────┐
       ▼                               ▼
┌──────────────┐                ┌──────────────┐
│   VOLUME 0   │                │   VOLUME 1   │
│ (Bytes 0-2GB)│                │ (Bytes 2-4GB)│
│ backup.tar.1 │                │ backup.tar.2 │
└──────────────┘                └──────────────┘
       │                               │
       └───────────────┬───────────────┘
                       ▼
          Uploaded to AWS S3 / Azure
```

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
Cloud storage providers (like AWS S3) prefer uploads split into parts. TarTape can divide your archive into virtual chunks on-the-fly:

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

            # In-flight checksum is calculated passively during upload (zero extra I/O)
            print(f"Part uploaded - {volume.checksum_algorithm}: {volume.checksum}")
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

### 5. Surgical Single-File Recovery via HTTP Range (Without Full Downloads)

Imagine you uploaded a 50 GB dataset in 1 GB parts to S3 (as shown in **Example 2**). Months later, on a remote server or Lambda function, you only need to retrieve a single report (`annual_2024.pdf`).

You **do not need the original 50 GB dataset** or a local copy of the archive. You only need the lightweight `.tartape` index file to calculate the exact HTTP Range headers and download only that file:

```python
import requests
import tartape

# Open index in offline zero-disk mode (read-only safe)
catalog = tartape.open_catalog("./my_dataset.tartape", read_only=True)

# Locate the file within the 1 GB slicing scheme used during upload
# (Works even if the layout was never explicitly saved in the DB!)
location = catalog.locate("my_dataset/reports/annual_2024.pdf", tag_or_size="1GB")

print(f"File size: {location.file_size:,} bytes")
print(f"Spans across {len(location.fragments)} cloud volume(s)")

# Download only the exact bytes from the cloud
for fragment in location.fragments:
    part_url = (
        f"https://my-bucket.s3.amazonaws.com/backups/part_{fragment.volume_index}.tar"
    )

    # 'fragment.range_header' provides precomputed headers, e.g.:
    # {'Range': 'bytes=1048576-2097151'}
    response = requests.get(part_url, headers=fragment.range_header)

    with open("restored_annual_2024.pdf", "ab") as f:
        f.write(response.content)

# Note: If your archive was uploaded as a single monolithic TAR file,
# simply omit the size parameter: catalog.locate("my_dataset/reports/annual_2024.pdf")
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
TarTape ensures what you stream matches what was originally scanned. You can inspect your dataset using full audits or ultra-fast canary spot-checks:

```python
import tartape

with tartape.open("./my_dataset") as tape:
    # Full 100% audit (default) returning a rich diagnostic report
    report = tape.verify()

    if not report:
        print(f"Audit failed! Found {report.error_count} discrepancies:")
        for error in report.errors:
            print(f"  - [{error.reason}] {error.arc_path}: {error.message}")
    else:
        print(
            report.summary()
        )  # e.g., "Verification [DEEP] PASSED: 4,200/4,200 tracks checked in 18.4ms."

    # Or run an ultra-fast Canary check (<15ms) across boundary and volatile sentinels:
    # canary_report = tape.verify(deep=False)
```

### 9. Recording with File Checksums and Metadata Inspection
You can index directory metadata instantly (default), or pre-calculate cryptographic file checksums using SHA-256 or MD5:

```python
import tartape

# Fast metadata-only scan (default)
tape = tartape.record("./my_dataset")

# Or calculate file checksums upfront using SHA-256:
tape = tartape.record("./my_dataset", checksum="sha256", auto_truncate=True)

# Rich inspection metrics:
print(f"Algorithm:       {tape.checksum_algorithm}")  # 'sha256'
print(f"Has file hashes: {tape.has_file_checksums}")  # True
print(f"Raw data size:   {tape.data_size:,} bytes")  # Net uncompressed payload
print(
    f"Total TAR size:  {tape.total_size:,} bytes"
)  # Stream size (with headers & padding)
print(
    f"TAR overhead:    {tape.overhead_size:,} bytes"
)  # Header + padding byte overhead
```

---

## Handling Deeply Nested Directories (`auto_truncate`)

The standard TAR specification (USTAR) restricts individual folder and file names to 100 bytes. In deeply nested projects (like complex web projects or cache trees), some paths may exceed this limit.

By default, TarTape aborts with a clear report if a path violates this standard. If you want TarTape to handle this automatically, enable `auto_truncate=True`:

```python
tape = tartape.record("./my_dataset", auto_truncate=True)
```

**How it works:**
* Long path components are deterministically shortened using a hash suffix (e.g., `very_long_folder_name_a1b2c3d4`).
* The resulting archive remains 100% compliant with standard `tar` tools.

> **Good to know:** When extracting using standard system tools (`tar -xf`), those specific folders will use the shortened name. However, **the full original path is always preserved in your `.tartape` index file**, ensuring you never lose your original directory metadata.

---

## Important Things to Know

* **Determinism Scope:** Running `record()` on the same folder produces the exact same byte sequence and hash when executed on machines running the **same operating system** (e.g., Linux to Linux).
* **Fail-Fast Safety:** If a file is modified, resized, or deleted while TarTape is streaming, the process stops immediately and raises an error. It will never silently send mismatched or corrupt archives.

---

## API Summary

| Function / Method | Mode | What it does |
|:---|:---:|:---|
| `tartape.record(path, ...)` | Write | Scans a folder, creates its `.tartape` sidecar catalog, and returns an active `Tape`. |
| `tartape.open(path)` | Streaming | Opens an already-indexed folder for streaming. Keeps DB session open inside context manager. |
| `tartape.open_catalog(path, read_only=False)` | Zero-Disk | Opens only the index file (no source files needed) to inspect contents, layouts, or run GPS queries. |
| `tape.as_file(buffer_size=64KB)` | Streaming | Returns a standard file-like object with `read()` and `seek()` to stream the archive. |
| `tape.play(start_offset=0)` | Streaming | Yields raw byte chunks directly with byte-accurate resumption. |
| `tape.iter_volumes(size="1GB")` | Streaming | Yields the archive split into file-like slices (`Volume`) for cloud multipart uploads. |
| `tape.get_volume(index, size="1GB")` | Streaming | Retrieves a specific volume directly without iterating through previous parts. |
| `tape.verify(deep=True)` | Integrity | Checks if local files match the T0 inventory snapshot (supports `<15ms` Canary checks). |
| `tape.locate(arc_path, ...)` | Zero-Disk | Shortcut to locate a file's GPS coordinates using the tape's catalog. |
| `catalog.locate(arc_path, tag_or_size=None)` | Zero-Disk | Returns GPS coordinates and precalculated `range_header` using a monolithic or ad-hoc virtual layout. |
| `volume.compute_checksum(algorithm=None)` | Streaming | Deliberately reads from disk to calculate the volume checksum upfront. |
