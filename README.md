# TarTape

TarTape is a TAR archive generation engine designed with a focus on **streaming** and explicit control over the archiving process.

It is built for environments where generating a full TAR file in memory or via temporary disk files is not feasible or desirable—such as data pipelines, web services, or resource-constrained systems.

Rather than replacing traditional TAR tools, TarTape offers a predictable and observable alternative for scenarios where data flow integrity is paramount.

[Leer versión en español (Spanish)](./README.es.md)

---

## Key Features

*   **True Data Streaming**  
    Generates a continuous byte stream, facilitating integration into pipelines where the final file doesn't need to reside on disk (e.g., direct cloud uploads).

*   **Memory Efficiency**  
    RAM consumption remains low and constant, regardless of the total archive size. This allows for processing massive data volumes predictably.

*   **Observability**  
    Unlike a "black box," TarTape emits events throughout the process. You can monitor exactly which file is being processed and react in real-time.

*   **Integrity First**  
    The engine verifies that files do not change size while being read (e.g., active logs). If a discrepancy is detected, it raises an explicit error to prevent silent archive corruption.


---

## Installation

```bash
pip install tartape
```

## Usage Examples

### Basic Usage: Generate a TAR file

In the simplest case, TarTape emits the TAR file bytes as a stream that can be written directly to a file.

```python
from tartape import TarTape, TarEventType

tape = TarTape()
tape.add_folder("./my_data")

with open("backup.tar", "wb") as f:
    for event in tape.stream():
        if event.type == TarEventType.FILE_DATA:
            f.write(event.data)
```


### Streaming with Monitoring and Control

TarTape exposes the archiving process through events, allowing you to observe what happens at each stage of the stream.

```python
from tartape import TarTape, TarEventType

tape = TarTape()
tape.add_folder("/var/log/app")

for event in tape.stream():
    if event.type == TarEventType.FILE_START:
        # Emitted before processing a file
        print(f"Archiving: {event.entry.arc_path} ({event.entry.size} bytes)")

    elif event.type == TarEventType.FILE_DATA:
        # Raw TAR bytes (headers, content, and padding)
        # These can be sent directly to a network socket or a bucket
        pass

    elif event.type == TarEventType.FILE_END:
        # Emitted when a file is finished
        # Includes metadata like the MD5 hash calculated during the read
        print(f"File completed. MD5: {event.metadata.md5sum}")

    elif event.type == TarEventType.TAPE_COMPLETED:
        print("TAR archive completed successfully.")
```