### ADR-005: Path Component Constraints (Strict Integrity vs. Format Limits)

#### Context
The USTAR standard allow paths up to 255 bytes by splitting them into `prefix` (155) and `name` (100) at a `/` separator. Theoretically, an archive could contain a file at `very_long_folder_name/file.txt` without including an explicit entry for the folder itself.

#### Decision
TarTape will enforce a **Strict Component Integrity** policy.
1. Every directory discovered during the recording phase **must** have its own independent TAR entry (Type '5').
2. This ensures that directory metadata (permissions, `mtime`) is captured and verified for integrity (ADR-002).

#### The Constraint Conflict
Because of this strict policy, a folder named `folder_of_120_bytes/` must be representable in its own header. Since it has no internal `/` to split the name, it must fit entirely in the 100-byte `name` field.
*   **Result:** A folder name longer than 100 bytes is illegal in TarTape, even if a standard `tar` tool could technically "skip" its header and store a nested file using the `prefix` field.

#### Rationale
Allowing the engine to skip directory headers for "long-named folders" would create a **"Metadata Black Hole"**:
*   The folder's integrity (ADR-002) could not be verified.
*   The stream would become inconsistent (some folders have entries, others don't).
*   Calculating offsets would become more complex as the engine would have to decide which directories to omit.
