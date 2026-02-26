# TarTape

**TarTape** is a streaming engine designed to turn massive folders into TAR archives with absolute predictability.

Standard archiving tools are dynamic: their internal structure changes depending on file names and sizes. This makes it impossible to know the exact layout of the archive until it is fully created.

**TarTape changes the rules.** It organizes your data into a "Master Tape" where every file entry follows a strict, fixed-size layout. This creates a **predictable stream** that solves the biggest challenges of large-scale data transfers:

*   **Resume interrupted uploads:** If a 10TB transfer fails at 4.2TB, TarTape knows the exact byte where it left off and can resume instantly without re-scanning the source.
*   **Verify Integrity:** It ensures the files you are streaming haven't changed since you first "recorded" the tape.
*   **Navigate the Stream:** Jump to any file or offset within the archive without having to process the preceding data.


---

## Installation

```bash
pip install tartape
```

## Usage Examples

### 1. Recording the Tape
Before streaming, you must "record" the folder. This creates a snapshot (stored in .tartape/index.db) that calculates the exact position of every file.

```python
from tartape import TapeRecorder

# This creates the .tartape metadata folder inside your dataset
recorder = TapeRecorder("./massive_dataset")
fingerprint = recorder.commit()

print(f"Tape ready. Fingerprint: {fingerprint}")
```

### 2. Basic Streaming

Once recorded, you can stream the folder to any destination (Cloud, Disk, Network). TarTape ensures the stream is exactly as described in the metadata.

```python
from tartape import Tape, TapePlayer

# Discover the tape and start the player
tape = Tape.discover("./massive_dataset")
player = TapePlayer(tape, source_root="./massive_dataset")

with open("backup.tar", "wb") as f:
    # Every event contains data chunks or metadata
    for event in player.play():
        if event.type == "file_data":
            f.write(event.data)
```

### 3. Resuming an Interrupted Stream

Because TarTape uses fixed positions, you can resume a multi-terabyte upload if it fails. You only need to know the last byte successfully sent.

```python
# If the previous upload failed at exactly 5GB...
start_offset = 5 * 1024 * 1024 * 1024

for event in player.play(start_offset=start_offset):
    # This skips all previous files and starts streaming
    # from the exact byte where the failure occurred.
    upload_to_cloud(event.data)
```

### 4. Professional Monitoring

TarTape acts as a "White Box," letting you see exactly which file is being processed and its calculated integrity.

```python
for event in player.play():
    if event.type == "file_start":
        print(f"Archiving: {event.entry.arc_path} at offset {event.metadata.start_offset}")

    elif event.type == "file_end":
        # Each file reports its MD5 hash calculated on-the-fly
        print(f"Done: {event.entry.arc_path} | Hash: {event.metadata.md5sum}")
```


## Observable Events

TarTape provides full visibility into the streaming process. Every chunk of data and every file transition is emitted as a structured event.

| Event Type | Description | Key Metadata Available |
|:---|:---|:---|
| `file_start` | Emitted before a file or directory enters the stream. | `entry` (metadata), `start_offset`, `resumed` (boolean). |
| `file_data` | Raw bytes belonging to the current file (header, body, or padding). | `data` (bytes). |
| `file_end` | Emitted after a file is fully processed and closed. | `entry`, `end_offset`, `md5sum` (if not resumed). |
| `tape_completed` | Emitted after the 1024-byte TAR footer is sent. | - |


## Constraints & Considerations

*   **Path Limit:** Maximum length of **255 characters**. This is a hard limit to ensure stream predictability.
*   **Anonymization:** User/Group IDs and names are scrubbed by default. This ensures privacy and consistent fingerprints across different environments.
*   **Standard Compatibility:** Generated archives are fully compatible with modern TAR tools (`tar`, `7-zip`, etc.).
*   **Supported Types:** Handles Files, Directories, and Symlinks. Sockets, Pipes, and Devices are ignored.
*   **Strict Integrity:** Any modification, addition, or deletion of files after the recording phase will invalidate the tape and abort the stream.
*   **Data Portability:** Designed for data movement and cloud streaming. Not intended for forensic OS backups where local ownership must be preserved.
