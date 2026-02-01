# TarTape

TarTape is a TAR archive generation engine designed with a focus on **streaming** and explicit control over the archiving process.

Most TAR tools are unpredictable; their headers grow and shrink. TarTape follows a golden rule: **Every file header measures exactly 512 bytes.** 

Whether you are archiving a small text file or a 10 Terabyte database dump, the structure remains constant. This makes your stream mathematically predictable, allowing you to calculate offsets and resume interrupted uploads with byte-precision.


---

## Installation

```bash
pip install tartape
```

## Usage Examples

### 1. Basic Streaming
The simplest way to use TarTape. Emits events that you can pipe anywhere.

```python
from tartape import TarTape, TarEventType

tape = TarTape()
tape.add_folder("./my_dataset")

with open("backup.tar", "wb") as f:
    for event in tape.stream():
        if event.type == TarEventType.FILE_DATA:
            f.write(event.data)
```

### 2. Resuming an Interrupted Stream
Because TarTape uses a deterministic inventory, you can resume a multi-terabyte upload if it fails at the 50th file.

```python
tape = TarTape(index_path="manifest.db") # The inventory is persistent
tape.add_folder("./massive_dataset")

# If the previous run failed at 'video_042.mp4'...
for event in tape.stream(resume_from="massive_dataset/video_042.mp4"):
    # This skips the header/body of finished files and continues 
    # from the very next byte of the next file.
    upload_to_cloud(event.data)
```

### 3. Professional Monitoring
TarTape acts as a "White Box", letting you see exactly what's happening.

```python
for event in tape.stream():
    match event.type:
        case TarEventType.FILE_START:
            print(f"Archiving: {event.entry.arc_path} at offset {event.metadata.start_offset}")
        
        case TarEventType.FILE_END:
            # Integrity check: Each file reports its calculated MD5 during the stream
            print(f"Done: {event.entry.arc_path} | Hash: {event.metadata.md5sum}")
            
        case TarEventType.TAPE_COMPLETED:
            print("Stream finished successfully.")
```
## Observable Events
You can react to the following events:

| Event Type | Description | Metadata Available |
|------------|-------------|--------------------|
| `FILE_START` | Before emitting a file header | `start_offset`, `entry` |
| `FILE_DATA` | Raw bytes (Header, Body, or Padding) | `data` |
| `FILE_END` | After file is fully processed | `end_offset`, `md5sum` |
| `TAPE_COMPLETED` | After the 1024-byte TAR footer | - |


## Technical Constraints & Defaults

*   **Path Length**: Maximum 255 characters (to maintain the 512-byte header contract).
*   **File Types**: Supports Files, Directories, and Symlinks. Sockets, Pipes, and Devices are ignored.
*   **Anonymization (Default: On)**: By default, it scrubs local UID/GIDs and usernames to ensure privacy and hash consistency across different environments. *This can be disabled if local identity preservation is required.*
*   **Format**: Uses GNU TAR format extensions for large file support (>8GiB).