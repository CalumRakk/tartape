# ADR-004: Large File Support via Base-256 Encoding (GNU-style)

## Status
Accepted

## Context
The **USTAR (POSIX.1-1988)** standard, which `tartape` implements to ensure universal compatibility, uses a 12-byte field to store the file size (`size`) in octal format. Since the last byte must be a terminator (NULL or space), only 11 characters remain for the number, imposing a maximum limit of $8^{11} - 1$ bytes—exactly **8 GiB**.

Any file exceeding this size cannot be represented in pure USTAR. Given that `tartape`'s vision is to serve as a **Data Streaming** engine for massive data environments (Terabytes), this limitation is a critical blocker for processing database dumps, disk images, or extensive log files.

## Decision
`tartape` will adopt a **hybrid encoding strategy** for the size field, following the *de facto* standard established by GNU Tar:

1.  **Standard Mode (Octal):** If the file size is **less than 8 GiB**, the standard USTAR octal representation will be used. This ensures human readability and full compatibility with legacy systems.
2.  **Extended Mode (Base-256):** If the size is **equal to or greater than 8 GiB**, the first bit of the `size` field will be set to `1` (indicating binary format), and the remaining bits will store the size in Big-Endian format.

### Architectural Justification
This decision is based on preserving the core pillars of the project:

*   **Invariant Header:** Unlike the PAX (POSIX.1-2001) standard, which introduces additional and variable-sized metadata blocks, the GNU extension keeps each entry's header strictly at **512 bytes**.
*   **Predictive Offset Calculation (ADR-001):** By keeping the header size constant, the `SqlInventory` can calculate the exact offset of any file within a Terabyte-scale stream through a simple arithmetic operation. This allows:
    *   Identifying exactly in which volume or position a file resides without processing the preceding stream.
    *   Facilitating partial downloads (e.g., HTTP Range Requests) for specific files within the TAR.
*   **Stream Simplicity:** The byte flow remains linear (`Header -> Body -> Padding`), facilitating observability and avoiding complex conditional logic on the receiver's end.

## Consequences

*   **Positive:**
    *   **Virtually Infinite Capacity:** Support for files up to $2^{95}$ bytes, removing size restrictions for Big Data.
    *   **Constant Offset Calculation:** Metadata "overhead" per file is always fixed (512 bytes), simplifying volume resumption and random access.
    *   **Industrial Compatibility:** Although an extension, Base-256 encoding is supported by GNU Tar, BSD Tar, and the Python standard library (`tarfile`).

*   **Negative:**
    *   **Departure from Pure USTAR:** Archives containing elements > 8 GiB cannot be correctly interpreted by extremely old archiving systems that do not support binary extensions. This is considered an acceptable trade-off given the project's "Cloud Era" focus.

## Rejected Alternatives

### PAX Format (POSIX.1-2001)
While it is the official standard for overcoming USTAR limits, it was rejected due to its impact on stream predictability. PAX inserts extra metadata blocks of variable size. This would make offset calculations for resumption dependent on each file's metadata content, complicating volume logic and degrading the structural observability that `tartape` seeks to provide.
