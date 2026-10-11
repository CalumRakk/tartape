# ADR-006: Pluggable Integrity Checksums and Two-Tier Hashing

## Status
Accepted

## Context
Originally, TarTape implemented an asymmetric hashing strategy:
1. File-level checksums ($T_0$) were optional (`calculate_hashes=False`), but hardcoded to MD5 when enabled.
2. Volume-level checksums ($T_1$) were forced to MD5 during in-flight streaming, and accessing `volume.md5sum` before transmission silently triggered a full-disk read (`_calculate_manually`) without the caller's consent.

This created multiple issues:
* **FIPS Compliance Violations:** Hardcoded MD5 usage caused failures in security-hardened environments (FIPS 140-2/3) and static analysis alerts.
* **Semantic & Performance Ambiguity:** Querying what seemed to be a lightweight property (`volume.md5sum`) could unexpectedly freeze processes by reading gigabytes off disk.
* **Modern Cloud Misalignment:** Contemporary cloud providers (AWS S3 Flexible Checksums, Google Cloud Storage, Azure) support and prefer SHA-256 and CRC32C over legacy MD5.

## Decision

We decouple content verification ($T_0$) from transport validation ($T_1$) and standardize on a clean, pluggable checksum architecture:

### 1. Unified $T_0$ Checksum Parameter
The redundant `calculate_hashes: bool` and `hash_algorithm: str` parameters in `record()` are replaced by a single polymorphic parameter:
* `checksum=False` (default): Fast metadata-only scan. File contents are not read.
* `checksum=True`: Enables file-level hashing using `"sha256"` as the secure standard.
* `checksum="sha256"` / `checksum="md5"`: Explicit algorithm selection.

The chosen algorithm and payload size are committed directly to `TapeMetadata` (`checksum_algorithm`, `has_file_checksums`, `data_size`).

### 2. Pure Passive In-Flight Volume Checksums ($T_1$)
The `Volume.checksum` property is made strictly passive and non-blocking:
* Returns the hexadecimal digest **only if** the volume was read linearly to completion via `read()`, or if it was already committed in the catalog database.
* Returns `None` if the volume has not been streamed, if the transfer was partial, or if non-linear `seek()` calls invalidated stream tracking.
* **Under no circumstances does `.checksum` perform silent disk I/O.**

### 3. Explicit On-Demand Calculation
When a volume checksum must be computed prior to transmission or after random seeks, callers must invoke the explicit action method:
```python
vol_checksum = volume.compute_checksum(algorithm=None)
```
If the source files are absent on disk (e.g. offline catalog mode), this method raises `SourceNotFoundError`.

### 4. Cryptographic Independence for Text Formatting
Internal non-cryptographic hashes used for USTAR path shortening (`shorten_path_ustar`, `truncate_component_safe`) must explicitly supply `usedforsecurity=False` in Python 3.9+ to guarantee compliance in FIPS environments.

## Consequences

* **Positive:**
  * Clean, predictable API with zero hidden disk reads.
  * Native compatibility with FIPS-enforced operating systems and enterprise security scanners.
  * Flexibility for modern cloud destinations using SHA-256 while preserving compatibility with legacy MD5 systems.
* **Negative:**
  * `Volume.checksum` returns `None` prior to streaming, requiring downstream code to call `compute_checksum()` if upfront validation is needed.
