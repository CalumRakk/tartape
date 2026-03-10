# TarTape

**TarTape** is a Python streaming library that generates deterministic TAR archives on-the-fly, transmitting directory contents without requiring intermediate local storage.

It is purpose-built for **cloud-native backups and large-scale data movement** where you need to stream terabytes of data directly to remote storage (S3, Azure, GCP). It eliminates the need to duplicate local disk space and provides the unique ability to resume failed uploads instantly from the exact byte they stopped.

### Key Features

*   **On-the-Fly Archiving:** Generates the TAR stream directly in memory during transmission. It processes files in chunks, eliminating the need for local disk space to store the final archive.
*   **Deterministic Output:** Ensures that the same source files always produce the exact same byte sequence and hash, regardless of the host machine or user environment.
*   **Byte-Level Resumption:** Allows interrupted transfers to be resumed from an exact byte offset. The engine performs a quick metadata check to ensure source files haven't changed, avoiding the need to re-read previously transmitted data.
*   **Strict Integrity:** Monitors file state safely. If a source file is modified after the initial scan, the stream aborts automatically to prevent generating a corrupted or misaligned archive.
*   **Logical Volume Slicing:** Exposes the continuous TAR stream as a sequence of fixed-size, file-like objects, making it easy to integrate with multipart upload APIs (like AWS S3 or Azure Blobs).

---

## Installation

```bash
pip install tartape
```


## Usage Examples

### 1. Recording the Tape
Before streaming, you must "record" the directory state. This creates a lightweight index in `.tartape/index.db`.

```python
import tartape

# Scan the dataset and generate the integrity catalog
tape = tartape.create("./massive_dataset")

print(f"Fingerprint: {tape.fingerprint}")
print(f"Total stream size: {tape.total_size} bytes")
```

### 2. Basic Streaming
You can consume the TAR archive as a raw byte generator, ideal for HTTP uploads or socket transmissions.

```python
import requests
import tartape

# Retrieve the previously recorded tape
tape = tartape.get_tape("./massive_dataset")

def data_generator():
    # 'play' emits events. We filter for 'file_data' to get raw bytes.
    for event in tape.play():
        if event.type == "file_data":
            yield event.data

# Send the full TAR stream via HTTP without saving it to disk
requests.put("https://storage.com/backup.tar", data=data_generator())
```

### 3. Volume Slicing (Cloud Slicing)
Ideal for services like AWS S3 or Azure Blobs that prefer fixed-size parts.

```python
import tartape

tape = tartape.get_tape("./massive_dataset")

# Split the stream into 1GB logical volumes
for volume, manifest in tape.iter_volumes(size=1024**3):
    # 'volume' behaves like an open file (read, seek, tell)
    # It must be used as a context manager to initialize the stream properly
    with volume:
        upload_to_s3(key=volume.name, body=volume)
```

### 4. Byte-Perfect Resume
If a transfer is interrupted, you can resume it from the exact byte where it left off.

```python
import tartape

# Suppose logs indicate that 45,678,912 bytes were sent before the error
LAST_BYTE_SENT = 45678912

tape = tartape.get_tape("./massive_dataset")

# 'play' will instantly jump to the requested offset without re-reading previous files
for event in tape.play(start_offset=LAST_BYTE_SENT):
    if event.type == "file_data":
        socket.send(event.data)
```

### 5. Integrity Verification
Check if local files have mutated (mtime or size) relative to the recorded index.

```python
with tartape.open("./massive_dataset") as tape:
    # 'verify' performs a random spot-check for quick detection.
    # Use verify(deep=True) for a full bit-by-bit audit of every file.
    try:
        tape.verify()
        print("Dataset is consistent with the index.")
    except Exception as e:
        print(f"Integrity compromised: {e}")
```



## Observable Events

TarTape provides full visibility into the streaming process. Every chunk of data and every file transition is emitted as a structured event.

| Event Type | Description | Key Metadata Available |
|:---|:---|:---|
| `file_start` | Emitted before a file or directory enters the stream. | `entry` (metadata), `start_offset`, `resumed` (boolean). |
| `file_data` | Raw bytes belonging to the current file (header, body, or padding). | `data` (bytes). |
| `file_end` | Emitted after a file is fully processed and closed. | `entry`, `end_offset`, `md5sum` (if not resumed). |
| `tape_completed` | Emitted after the 1024-byte TAR footer is sent. | - |


## Integrity Rules & Constraints

*   **T0 State Consistency:** If a file changes after it has been recorded, the engine will abort the stream to prevent generating a corrupt or mismatched archive.
*   **Anonymization:** User/Group IDs (UID/GID) are scrubbed by default. This ensures that the same dataset generates the exact same byte stream (and Hash) regardless of the host machine or user.
*   **Path Limits:** For universal compatibility and fixed-offset predictability, paths are limited to **255 bytes** total, and individual folder/file names are limited to **100 bytes**.



*Compatible with Python 3.10+ and any standard extraction tool (tar, 7-zip, etc).*
