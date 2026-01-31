# ADR-003: Identity Anonymization and Functionality Preservation

## Status
Accepted

## Context
Traditional TAR tools were designed for **System Backups**, where preserving the exact ownership (UID/GID and usernames) is vital to restore an operating system to its original state. However, `tartape` is designed for **Data Streaming** and cloud portability.

In a streaming context, local User IDs (e.g., UID 1000) are "environmental noise." They are specific to the source machine and have no meaning on the destination system or cloud storage. Furthermore, including these IDs and usernames in the archive:
1.  **Leaks Privacy:** It exposes internal system usernames and folder structures.
2.  **Breaks Determinism:** The same file processed by two different users will produce different TAR headers (and different hashes), even if the content is identical.
3.  **Complicates Portability:** Extracting a file with a foreign UID can lead to permission issues or unintended ownership on the receiver's side.

Crucially, this "Identity" must not be confused with "Functionality" (File Mode/Permissions). An executable script must remain executable regardless of who owns it.

## Decision
`tartape` will decouple **Identity** from **Functionality** to prioritize privacy and portability:

1.  **Identity Flattening (Anonymization):** By default, the engine will "flatten" ownership metadata. All entries will be recorded as belonging to a generic user (UID 0, GID 0, "root") or empty values. This ensures the generated stream is anonymous and reproducible across different environments.
2.  **Functionality Preservation (Mode):** The file permissions (the `mode` bits, such as 0755 for executables or 0644 for documents) will be faithfully preserved. This ensures that the files remain usable and retain their intended behavior upon extraction.
3.  **Explicit Control:** While anonymization is the preferred path for data streaming, the engine will allow developers to override these values if a specific identity is required for a particular use case.

## Consequences

*   **Positive:**
    *   **Enhanced Privacy:** Archives do not leak local system information or usernames.
    *   **Bit-for-bit Determinism:** The same set of files will produce the exact same TAR hash regardless of the user running the process.
    *   **Universal Portability:** Archives can be extracted on any system without creating "orphaned" UIDs or ownership conflicts.
    *   **Functionality:** Software and scripts remain executable after being streamed and extracted.

*   **Negative:**
    *   **Not for System Cloning:** This tool cannot be used to perform a full OS backup where preserving local user ownership is mandatory.
    *   **Metadata Loss:** The original creator's identity is intentionally discarded during the process.
