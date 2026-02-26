# The TarTape Paradigm

Most TAR tools were designed to back up operating systems (preserving users, local permissions, etc.).

**TarTape** was born for something simpler: **moving data from one place to another over the internet.**

We don't try to be a Swiss Army Knife. We want to be a conveyor belt that never stops, wastes no memory, and—if the connection drops—knows exactly at which byte it left off to resume working.

### The Golden Rule: The 512-Byte Contract
In the TAR format, every file starts with a metadata header. In many programs, this header can grow unpredictably.

In TarTape, we have decided that **the header will always measure exactly 512 bytes.**

This simplicity is our greatest advantage:
*   If you know how many files there are and their size, you know exactly where each one starts in the stream.
*   This allows you to "jump" to any point in a multi-Terabyte archive without having to read everything from the beginning.
*   To achieve this without breaking the old 8GB limit (USTAR), we use a special encoding (GNU Base-256) that allows us to store giant sizes in the same space as always.

### The Three Pillars

#### I. Determinism (Order Matters)
If you process the same files twice, the result must be bit-for-bit identical. That is why TarTape sorts files alphabetically by default. If the flow is predictable, it is recoverable.

#### II. Privacy by Default (Anonymity)
A cloud database doesn't care which user created the file on your laptop. TarTape scrubs usernames and local UIDs, keeping only what matters: content and execution permissions. This makes the final file cleaner and ensures the hash is always the same.

#### III. The Promise (T0 Inventory)
Before emitting any bytes, TarTape makes a "promise" (Inventory): "This file measures X and was modified at time Y".
If, while reading the file to send it, the disk tells us the file has changed, **TarTape stops immediately.** We prefer to fail fast rather than generate a corrupt file in silence.

### Conclusion
`tartape` is a specialized tool. It intentionally trades the ability to clone operating systems to become an exceptionally predictable and observable engine for data transport.
