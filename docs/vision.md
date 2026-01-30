# The TarTape Paradigm

### 1. Introduction: Data Streaming vs. System Backup
`tartape` is not just another implementation of the TAR standard. While it adheres to the USTAR format to ensure universal compatibility, its architecture stems from a need distinct from traditional tools like GNU `tar`.

While traditional TAR was designed for **System Backup** (preserving the exact state of a physical machine), `tartape` is designed for **Data Streaming** (transporting information efficiently, securely, and reproducibly across networks and clouds).

### 2. Approach Comparison

| Feature | System Backup (Traditional) | Data Streaming (TarTape) |
| :--- | :--- | :--- |
| **Primary Goal** | Restore a complete Operating System. | Synchronize and transport data between nodes. |
| **Identity** | Preserves local UID/GID (vital for the OS). | Anonymizes Identity (privacy and portability). |
| **Functionality** | Preserves permissions (execution, read). | Preserves permissions (user convenience). |
| **Determinism** | Order is irrelevant (single final file). | Order is critical (defines offsets for resumption). |
| **Integrity** | Based on the moment of reading. | Based on an "Instant Snapshot" (T0). |

### 3. The Three Pillars of TarTape

#### I. Determinism as a Contract
In a streaming environment, especially when the flow is split into volumes or parts, the file order is not a suggestion—it is a **Sequence Contract**.
If file A is processed before file B, this defines the exact byte where each one begins. `tartape` guarantees that this order is persistent to allow any streaming operation to be resumable bit-by-bit.

#### II. Conscious Portability
`tartape` distinguishes between **Identity** and **Behavior**:
*   **Identity (UID/GID):** Considered "environmental noise" from the source system. It is flattened (anonymized) to ensure the backup does not leak private data and is reproducible on any machine.
*   **Behavior (Permissions/Mode):** Faithfully preserved. An executable file at the source must remain executable at the destination.

#### III. The "T0 Image" (Absolute Truth)
For `tartape`, the archiving process is divided into two phases:
1.  **T0 (Inventory):** The "Promise" is captured (Name, Size, mtime).
2.  **T1 (Streaming):** The promise is executed.

If at time T1 the disk contradicts the promise made at T0 (the file mutated or changed size), `tartape` aborts the operation. This protects the integrity of the TAR structure and prevents the receiver from receiving inconsistent data.

### 4. Conclusion
`tartape` sacrifices the standard capability of cloning operating systems in exchange for becoming a surgical tool for data transport. It is an engine designed for the cloud era, where **observability**, **resumability**, and **determinism** are more important than fidelity to local user IDs.
