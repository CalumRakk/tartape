# ADR-002: Integrity based on T0 Snapshot (Size and mtime)

## Status
Accepted

## Context
In a TAR archive generation engine based on streaming, the size of each file must be declared in the header **before** starting to transmit its content. The TAR format is extremely sensitive to block alignment: if the engine promises 100 bytes but the file on disk changes during reading and delivers 101 bytes, that extra byte shifts the entire subsequent structure of the TAR file, turning the rest of the stream into corrupt and unreadable data.

Unlike a traditional backup where one might try to "reflect the present," `tartape` is used in network environments and volume systems where consistency is critical. If a file mutates (changes in size or modification time) between the moment it was inventoried and the moment the stream is generated, the original "snapshot" is no longer valid, and any offset previously calculated to resume the upload becomes invalidated.

## Decision
`tartape` will implement an **"Immutable Integrity Contract"** model based on the initial state of the files:

1.  **Promise Validation:** The engine will treat the size (`size`) and modification time (`mtime`) metadata captured during the initial inventory (T0) as the only acceptable truth.
2.  **Runtime Verification:** During the streaming process (T1), right before and during the reading of each file, the engine will compare the current disk state with the T0 "promise."
3.  **Explicit Failure (Abort):** If any discrepancy is detected (the file grew, shrank, or was modified), `tartape` will stop the process immediately by raising an integrity exception. No attempt will be made to repair the flow or fill with empty data, ensuring that the receiver never receives a structurally inconsistent TAR file.

## Consequences

*   **Positive:**
    *   **Structural Security:** Guarantees that the TAR block alignment is always perfect, preventing an error in one file from corrupting all subsequent ones.
    *   **Volume Consistency:** Ensures that file fragments (offsets) are mathematically consistent, allowing for reliable resumptions.
    *   **Collision Detection:** Alerts the user about external processes (active logs, databases) that might be compromising the backup quality.

*   **Negative:**
    *   **Sensitivity to Active Files:** The archiving process will fail if an attempt is made to process files that are being actively modified by the operating system.
    *   **Need for External Management:** Forces the developer to decide how to handle these failures (e.g., retrying the entire operation or excluding the conflicting file in a second pass).
