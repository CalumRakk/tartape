# TarTape

**TarTape** is a streaming engine designed to turn massive directories into deterministic TAR archives on-the-fly, without requiring intermediate storage.

It is purpose-built for **cloud-native backups and large-scale data movement** where you need to stream terabytes of data directly to remote storage (S3, Azure, GCP). It eliminates the need to duplicate local disk space and provides the unique ability to resume failed uploads instantly from the exact byte they stopped.

### Why is it useful?
*   **Zero-Copy Streaming:** Generates the TAR stream "in-flight" while transmitting. If your dataset is 100GB, you transmit 100GB without using a single extra GB of local cache.
*   **Byte-Level Resume:** If a 500GB upload fails at 80%, TarTape knows exactly at which byte the error occurred. You can resume the stream from that specific offset without re-scanning the source.
*   **Logical Volume Slicing:** Easily split a massive stream into fixed-size volumes (e.g., 5GB parts) to meet cloud provider upload limits, while maintaining a single valid TAR structure.
*   **Stream Navigation:** Jump to any file or offset within the resulting archive without having to process or read the preceding data.


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

### 2. Direct Streaming (Single-file Upload)
If you don't need to split the archive, you can consume the byte stream directly.

```python
import requests
import tartape

tape = tartape.Tape("./massive_dataset")

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

# Split the stream into 1GB logical volumes
for volume, manifest in Tape("./massive_dataset").iter_volumes(size=1024**3):
    # 'volume' behaves like an open file (read, seek, tell)
    upload_to_s3(key=volume.name, body=volume)
```

### 4. Byte-Perfect Resume
If a transfer is interrupted, you can resume it from the exact byte where it left off.

```python
# Suppose logs indicate that 45,678,912 bytes were sent before the error
LAST_BYTE_SENT = 45678912

with tartape.open("./massive_dataset") as tape:
    # 'play' will instantly jump to the requested offset
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
