# ADR-002: Integrity based on T0 Snapshot (Size and mtime)

## Status
Accepted (Amended for v3.0: Zero-Pollution Sidecar Architecture)

## Context
In a TAR archive generation engine based on streaming, the size of each file must be declared in the header **before** starting to transmit its content. The TAR format is extremely sensitive to block alignment: if the engine promises 100 bytes but the file on disk changes during reading and delivers 101 bytes, that extra byte shifts the entire subsequent structure of the TAR file, turning the rest of the stream into corrupt and unreadable data.

Unlike a traditional backup where one might try to "reflect the present," `tartape` is used in network environments and volume systems where consistency is critical. If a file mutates (changes in size or modification time) between the moment it was inventoried and the moment the stream is generated, the original "snapshot" is no longer valid, and any offset previously calculated to resume the upload becomes invalidated.

## Decision
`tartape` will implement an **"Immutable Integrity Contract"** with specific rules for different entry types:

1. **File Integrity:** For regular files, the engine treats `size` and `mtime` captured at T0 as the absolute truth. Any discrepancy at T1 (playback) will trigger an immediate abort.
2. **Structural Integrity (Directories):**
   * Changes in `mtime` across any directory are treated as structural violations (indicating that files were added, deleted, or renamed) and trigger an immediate abort.
   * *(See Amendment below regarding the deprecation of the Root Directory Exception).*
3. **Strict Mode:** The engine operates in a "Fail-Fast" mode. It does not attempt to repair the stream or skip inconsistent files; it stops to guarantee that the receiver never gets a partial or misleading archive.
4. **Exclusion Rules:** Standard OS and temporary artifacts (e.g., SQLite WAL/SHM temporary files, `.DS_Store`) are globally excluded from integrity checks and the resulting data stream.

---

## Amendment (v3.0 - Zero-Pollution Sidecar Architecture)

### Deprecation of the "Root Directory Exception"
* **Historical Behavior (v1.x - v2.x):**
  The engine originally created an internal metadata directory (`.tartape/`) inside the source folder. Because creating or updating this directory naturally modified the root folder's `mtime`, TarTape exempted the root directory from `mtime` verification to prevent self-invalidating the snapshot.
* **Current Behavior (v3.0+):**
  TarTape transitioned to a **Zero-Pollution Sidecar Model**. Catalog databases are constructed in isolated temporary storage and sealed into a standalone sidecar file (`<directory>.tartape`) located alongside the source folder (or at an explicit external path).
* **Consequence of the Amendment:**
  The source directory is now treated as strictly **Read-Only** and is never modified by the engine. Consequently, the historical root directory exception has been deprecated. All directory metadata, including the root directory, is verified strictly against the T0 baseline.

---

## Consequences

* **Positive:**
  * **Structural Security:** Guarantees that TAR block alignment is always perfect, preventing an error in one file from corrupting all subsequent ones.
  * **Volume Consistency:** Ensures that file fragments (offsets) are mathematically consistent, allowing for reliable resumptions.
  * **Collision Detection:** Alerts the user about external processes (active logs, background writes) that might compromise backup quality.
  * **Read-Only Filesystem Compatibility (v3.0):** Preserves snapshot integrity on read-only network shares, ZFS/Btrfs snapshots, and immutable disks.

* **Negative:**
  * **Sensitivity to Active Files:** The archiving process will fail if an attempt is made to process files that are being actively modified by the operating system.
  * **Need for External Management:** Forces the developer to decide how to handle these failures (e.g., retrying the operation or excluding the conflicting path).

---

## Rejected Alternatives

### Structural Integrity Only (Ignoring `mtime` changes)

During the design phase, we considered allowing the engine to process files even if their modification time (`mtime`) had changed since the T0 snapshot, provided their size (`size`) remained identical.

**Context:**
Technically, if the file size does not change, the TAR block alignment remains intact, and the archive is structurally valid and extractable. This would seemingly increase the resilience of the stream against minor filesystem activity.

**Reason for Rejection:**
We explicitly rejected this "relaxed" approach due to its negative semantic implications:

1. **The "Forensic Lie":** The TAR Header is written *before* the file content is read. If we accept a file with a changed timestamp, the resulting archive will contain a Header with the old timestamp (T0) but content from a later time (T1). This creates an impossible-to-debug artifact where the metadata contradicts the actual data history.
2. **Downstream Cache Corruption:** Many synchronization tools (like `rsync`) and deployment caches rely on the `(size, mtime)` tuple to detect changes. By generating an archive with "old time" but "new content," we risk tricking downstream systems into skipping necessary updates.
3. **False Sense of Security:** A file can change its content without changing its size (e.g., toggling a boolean flag or a single pixel). Ignoring the `mtime` change violates the atomicity of the snapshot.

**Conclusion:**
Semantic integrity (truthfulness of metadata) is as critical as structural integrity. The engine must enforce a strict "Fail Fast" policy on *any* metadata discrepancy found between T0 and T1.
