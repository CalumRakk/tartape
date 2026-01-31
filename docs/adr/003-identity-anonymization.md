# ADR-003: Identity Anonymization and Functionality Preservation

## Status
Accepted

## Context
Traditional TAR tools were designed for **System Backups**, where preserving exact ownership (UID/GID and usernames) is vital for restoring an operating system to its original state. However, `tartape` is designed for **Data Streaming** and cloud portability.

Traditionally, the TAR format captures three layers of information:
1.  **Content:** The file's bytes.
2.  **Identity:** Ownership (UID/GID, user/group names).
3.  **Temporal State:** When it was last modified (`mtime`).

In the context of Data Streaming, the **Identity** layer is considered "environmental noise." A UID of 1000 on a local machine has no meaning in S3 storage or a target system. Furthermore, exposing local usernames compromises privacy and breaks determinism: the same file processed by two different users would generate a different byte stream (and a different hash).

However, the **Temporal State (`mtime`)** is different. For `tartape`, a change in `mtime` represents a potential mutation of the file. Ignoring or "flattening" time to force an identical hash (**Absolute Binary Determinism**) would directly conflict with **ADR-002 (Integrity Snapshot)**, which dictates that the engine must remain faithful to the state captured during inventory (T0).

## Decision

### 1. Identity Anonymization (Flattening)
To guarantee privacy and facilitate a consistent hash across environments with different users, `tartape` will:
*   Flatten `uid` and `gid` fields to `0`.
*   Flatten `uname` and `gname` fields to `"root"` (or empty values).
*   **Preserve the Mode (`mode`):** Permission and execution bits are kept, as they define the functional behavior of the file, not its identity.

### 2. Differentiation of Determinism
`tartape` defines two levels of determinism and chooses the former by design:

*   **Structural Determinism (TarTape's Goal):** If the content, order, and temporal state (`mtime`) of the files are identical, the resulting byte stream will be bit-for-bit identical. Identity anonymization is key to achieving this.
*   **Absolute Binary Determinism (Rejected by default):** The practice of forcing `mtime` to a constant value (e.g., 0 or Unix Epoch) to obtain the same hash even if files have been "touched" or recreated.

**TarTape’s Stance:** It is considered "illegal" to lie about time. If a file has a different `mtime` than the one in the inventory, it is considered a different file (or corrupt relative to the snapshot), and the engine must fail (ADR-002) instead of masking the change to save the hash.

## Consequences

*   **Positive:**
    *   **Privacy:** TAR files do not leak internal usernames or local permission structures.
    *   **Stable Hash:** The stream identifier (Hash) will be the same for a set of files as long as their state on disk is the same, regardless of who executes the process.
    *   **Portability:** Extracted files will not have orphaned UIDs on the destination system.

*   **Negative:**
    *   **Sensitivity to mtime:** If a version control system (like Git) changes `mtime` on a new checkout, `tartape` will generate a different hash. This is intentional to preserve the truthfulness of the filesystem state.
    *   **Not suitable for OS cloning:** It cannot be used for system backups where ownership (UID/GID) is critical for operating system functionality.