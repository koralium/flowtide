---
sidebar_position: 0
---

# Reservoir Storage System

## Design Goals

The reservoir storage system was designed with the following goals in mind:

### 1. Minimize number of writes to cloud storage to reduce cost

Cloud object stores (S3, Azure Blob Storage) charge per API operation (PUT, GET, DELETE). A streaming system that checkpoints frequently can generate a very large number of small writes, making storage costs dominate operational expenses.

**How the solution addresses this:**

* **Page packing into large immutable blobs.** Instead of writing each page individually, the system aggregates pages into large files (default 64 MB). If a single session produces enough data to fill a file on its own, it sends the file directly to the write queue, allowing multiple sessions to upload in parallel. Sessions that cannot fill a file by themselves contribute their pages into a shared `MergedBlobFileWriter`, which combines data from multiple sessions into a single file before writing. This turns potentially thousands of small writes into a handful of large writes.
* **Data and Checkpoint Bundle files.** When all changed data for a checkpoint fits within a single file, the system bundles the data pages, checkpoint metadata, and the checkpoint registry into one file (`DataCheckpointBundleFile`).
* **Checkpoint registry bundling on providers with listing support.** On storage providers that support file listing (e.g., local disk), the checkpoint registry is bundled into the checkpoint file itself, eliminating a separate registry write on every checkpoint.
* **Compaction merges fragmented files.** Over time pages are overwritten and files become sparse. The compaction process reads active pages from under-utilized files (below `CompactionFileSizeRatioThreshold`, default 33% of max size) and repacks them into new full-size files. The old files are then deleted, reducing the total file count and future GET operations.
* **Snapshot checkpoints reduce recovery reads.** Every `SnapshotCheckpointInterval` checkpoints (default 20), a snapshot checkpoint is written that contains all active page mappings. On recovery, only files from the latest snapshot onward need to be read, instead of replaying the entire checkpoint history.

### 2. Ensure data integrity across unreliable storage

Cloud storage can experience silent bit-rot, incomplete uploads, or corrupted downloads. A storage system for streaming state must detect and reject corrupted data to avoid propagating errors.

**How the solution addresses this:**

* **CRC32 per page.** Every individual page is checksummed with CRC32 when written. On every read, the checksum is verified before the data is returned to the caller. If the checksum does not match, a `FlowtideChecksumMismatchException` is thrown.
* **CRC64 per file.** Each data file has a CRC64 computed over all its bytes. During compaction and cache population, the file-level CRC64 is verified to detect corruption in the page-id index or any other structural damage that per-page CRC32 alone would not catch.
* **CRC64 on checkpoint and registry files.** Checkpoint files and the checkpoint registry each carry their own CRC64 values, forming a chain of trust: the registry CRC64 protects the list of checkpoints, and each checkpoint CRC64 protects its page-to-file mappings.
* **Self-healing local cache.** If a local cache read fails CRC verification, the cached file is purged and re-downloaded from cloud storage.

### 3. Achieve near-local-disk read latency while state lives in cloud storage

Reading directly from cloud storage on every page miss would add 10–100 ms of latency per request, making it impractical for interactive streaming workloads.

**How the solution addresses this:**

* **In-Flight Buffer for Read-Your-Own-Writes (RYOW).** Newly committed but not-yet-uploaded data is kept in an in-memory volatile buffer. Operators can read their own recently written pages immediately without waiting for the cloud upload to finish.
* **Local Disk Cache with on-demand fetching.** When a page is requested from a file not yet in the cache, the entire file is downloaded and stored on local disk. Subsequent reads for any page in that file are served directly from local disk at SSD speed.
* **DIRECT_IO support.** On Linux/Docker, the local cache can bypass the OS page cache using DIRECT_IO, preventing double-buffering and reducing the risk of OOM kills in memory-constrained containers.
* **Backpressure via bounded channel.** The in-flight buffer uses a `BoundedChannel` to limit pending writes. This prevents the engine from exhausting RAM if the storage backend is slow, while still allowing concurrent uploads.

### 4. Support fast recovery after failures

A streaming system must be able to restart and recover its state quickly. Reading all historical checkpoint files on every startup would be slow and expensive.

**How the solution addresses this:**

* **Snapshot checkpoints.** Periodic snapshot checkpoints contain the complete page-to-file mapping, so recovery only needs to read from the latest snapshot forward instead of the full history.
* **Checkpoint registry.** The registry file provides a directory of all checkpoint versions and their CRC64 values, eliminating the need for expensive list operations on cloud storage during initialization.
* **Immutable local cache survives failures.** Because cached files are immutable, they remain valid across stream restarts. The cache is not cleared on failure, reducing the volume of cloud API calls needed during recovery.

## Storage Hierarchy

Flowtide utilizes a 5-tier storage model to balance low latency with elastic cloud scalability:
1. RAM Pages (LRU): Hot, mutable state for active operator processing.
2. Disk Spillover: Local mutable storage for active checkpoints that exceed RAM capacity.
3. In-Flight Buffer: Immutable RAM buffer that holds data during the commit process, ensuring Read-Your-Own-Writes consistency before disk confirmation.
4. Local Disk Cache: Immutable, CRC-verified cache of 64MB blobs fetched on-demand from remote storage.
5. Persistent Storage: The definitive source of truth, typically hosted on Amazon S3 or Azure Blob Storage.

This document focuses on enabling tier 3 (In-Flight Buffer), 4 (Local Disk Cache) and 5 (Persistent Storage).

## In-Flight Buffer

The **In-Flight Buffer** is a volatile storage layer that bridges the gap between operator data production and persistent storage confirmation. It ensures **Read-Your-Own-Writes (RYOW)** consistency and provides backpressure during heavy write loads.

When an operator "commits" data, it is transitioned from a mutable state to an immutable state. Before this data is successfully written to the persistent storage, it resides in the **In-Flight Buffer**.

Key functions:

*   **Zero-Downtime Reads:** Allows operators to read newly produced data immediately, even if the background upload to S3/Azure is still in progress.
*   **Data Integrity:** Holds the immutable bytes used for the final **CRC64** calculation.
*   **Backpressure:** Uses a `BoundedChannel` (capacity 4) to limit the number of pending file writes, preventing the engine from exhausting RAM if the storage backend is slow.
*   **Page Aggregation:** Each session writes pages into its own `BlobFileWriter`. If a session fills a file (reaches `MaxFileSize`), it is sent directly to the write queue for parallel upload. Otherwise, the remaining pages are merged into a shared `MergedBlobFileWriter` on commit, combining data from multiple sessions into a single file to avoid writing many small files.

### Internal workflow

1. When an operator calls `Write`, the page data is serialized into a `BlobFileWriter` owned by the session.
   A reference to the written memory is added to a shared temporary lookup dictionary (`_temporaryPageLocations`) keyed by page id.
   If the session's file reaches `MaxFileSize` during writes, it is finalized and sent directly to the `CheckpointHandler` write queue (`AddCompleteBlobFile`). The session then creates a new `BlobFileWriter` for subsequent writes. This allows sessions that produce large amounts of data to fill and upload their own files in parallel.
2. On `Commit`, if the session's `BlobFileWriter` still has pages that did not fill a complete file, it is sent to `ReservoirPersistentStorage` as a non-completed file (`AddNonCompletedBlobFile`). These pages are merged into the shared `MergedBlobFileWriter`, which combines leftovers from multiple sessions. When the shared buffer reaches `MaxFileSize`, it is finalized and enqueued for upload.
3. When a page is read from the in-flight buffer, the data is returned from the temporary lookup.
   If the data spans multiple memory segments, it is copied into a contiguous buffer to ensure safe access.
4. A background write loop in `CheckpointHandler` picks up enqueued files from the bounded channel and writes them to persistent storage.
5. After a file has been written to storage, the `OnFileWritten` callback fires and the corresponding page ids are removed from the temporary lookup dictionary.

### On Stream Failure

On stream failure, the entire in-flight buffer is cleared. All temporary page locations are discarded and each session is reset.
Since no checkpoint was completed, none of the in-flight data needs to be persisted.

## Local Disk Cache

The **Local Disk Cache** is an immutable, on-demand storage layer that enables Flowtide to achieve near-local SSD performance while maintaining the state in cloud storage (S3/Azure). 

The cache acts as a "Warm" tier for large blobs. Instead of reading directly from the cloud for every page miss, Flowtide fetches entire blobs, verifies them, and serves subsequent page requests directly from the local disk.

Key features:

*   **On-Demand Fetching:** Blobs are only downloaded when a specific `PageId` is requested by an operator.
*   **Self-Healing:** If a local page read fails a CRC check, the cache automatically evicts the local file and re-downloads a fresh copy from the cloud.
*   **DIRECT_IO Support:** Optimized for Docker/Linux environments to bypass the OS Page Cache, preventing double-buffering and OOM kills.
*   **LRU Eviction with Weighted Frequency:** The cache maintains a linked-list based LRU index with frequency weights. Files that are accessed more frequently receive a higher weight and are evicted last. A proactive eviction loop runs periodically and starts evicting when the cache reaches 80% capacity.
*   **Bounded Size:** The cache size is limited by `MaxCacheSizeBytes` (default 10 GB). When space is needed for a new download, the least recently used files are evicted until enough space is available. If no files can be evicted (all are in active use), the download waits until space becomes available.

### Internal workflow

* **Cache misses** 
  1. When a page is requested on a file that does not exist in the cache, a download job is enqueued to a bounded channel.
  2. A background worker pool (default 2 workers) picks up the job, ensures there is enough cache space (evicting LRU entries if needed), and downloads the file from persistent storage.
  3. The downloaded file is written to local disk via the local cache storage provider.
  4. The file is registered in the LRU index with its file id, size, and CRC64 value.
* **Fetching a page from the cache**
  1. When a page is requested from the cache, the LRU entry is looked up and moved to the front of the list.
  2. The page data is read from the specified offset and length in the local file.
  3. The page data is verified against the stored CRC32 value to ensure data integrity.
  4. The page data is returned to the caller.
* **New data file**
  1. When a new data file is written to the persistent storage, it is also written to the local cache via `RegisterNewFileAsync`. This reduces latency for subsequent reads of newly written data.
* **Compaction reads**
  1. During compaction, files are read from the cache if they exist, but cache misses do not trigger a download. This avoids polluting the cache with data that will immediately be rewritten into a new file.

### On Initialization

When the cache initializes, it lists all data file ids already present on local disk and cross-references them against the active file information from the latest checkpoint. Files that are still active are registered in the LRU index. Files that are no longer referenced are deleted to reclaim disk space.

### On Stream Failure

On stream failure, the local disk cache is not cleared. Since all cached files are immutable, they remain valid across stream restarts and can be reused, reducing the number of cloud API calls required during recovery.


## Persistent Storage Layer

The **Persistent Storage Layer** is the final storage tier to ensure persistence. It provides long-term, durable storage for all streaming states, typically hosted in object stores like [Amazon S3](https://aws.amazon.com) or [Azure Blob Storage](https://azure.microsoft.com).

This layer is designed to be cloud-native, treating remote storage as an append-only log of immutable blobs.

Key features:

*   **Immutable Blobs:** Pages are packed into large files (default 64 MB) to saturate network bandwidth and minimize cloud API costs (PUT/GET).
*   **Checkpoint files:** Each checkpoint records which pages changed, which files were added or removed, and the page-to-file mappings (offsets, sizes, CRC32s). This enables fast point-in-time recovery.
*   **Checkpoint Registry:** A registry file that lists all checkpoint versions and their CRC64 values, eliminating the need for list operations on storage backends that do not support efficient listing.
*   **CRC64 Verification:** Every data file, checkpoint file, and registry file is hashed using CRC64 to guarantee integrity across different cloud providers.
*   **Compaction:** Files with a high ratio of invalidated pages (above `CompactionFileSizeRatioThreshold`) are compacted by reading the still-active pages and repacking them into new files. Files where all pages have been invalidated are deleted directly without compaction.
*   **Snapshot Checkpoints:** Every `SnapshotCheckpointInterval` checkpoints, a snapshot is written that contains all active page mappings. This bounds recovery time by allowing the engine to skip all checkpoint files before the latest snapshot.
*   **Zstd Compression:** Checkpoint data is compressed using Zstd before writing to reduce checkpoint file sizes. Bundle files are not compressed to keep CRC64 recalculation simple.
*   **Stream Version Management:** Multiple stream versions (e.g., different plan hashes) can coexist in storage. The `KeepLastStreamVersions` setting controls automatic cleanup of old versions.

## Checkpoint File

The checkpoint file is the core metadata structure that records all state changes for a single checkpoint version. It can be either an *incremental* checkpoint (recording only changes since the last checkpoint) or a *snapshot* checkpoint (recording the complete state).

The checkpoint file contains the following information:

* **Updated Page Count** - How many pages that have updated in the checkpoint
* **Deleted Page Count** - How many pages that have been deleted in the checkpoint
* **Updated File Count** - How many files that have been updated
* **Deleted File Count** - How many files that have been deleted
* **Next File Id** - The next data file id that can be used.
* Five columns for updated pages:
  * **PageId** - the identifier of the page that was updated
  * **FileId** - the file identifier that the page resides in the checkpoint
  * **OffsetInFile** - the byte offset where the page can be read from in the file
  * **Size** - Size of the page in bytes
  * **Crc32** - The crc32 value of the page, used for validation when reading. 
* **DeletedPageIds** - Array of page ids that have been deleted
* Seven columns for new/updated files (updates are only to invalidate replaced page data):
  * **FileIds** - the file identifiers of the files that have been updated.
  * **PageCount** - how many pages that exist in the file.
  * **NonActiveCount** - how many pages that have been invalidated in the file.
  * **Size** - Size of the file in bytes.
  * **Deleted Size** - Size of the invalidated pages in the file, used to check when to compact.
  * **Added at version** - Which checkpoint version the file was added in.
  * **Crc64** - Crc64 value of the file, used for validation.
* Two columns for deleted files:
  * **DeletedFileIds** - File ids that are no longer active.
  * **Deleted at version** - At what checkpoint version the file was marked as deleted.

The page ids are stored in sorted order, this is done to allow quick merging of multiple checkpoints
when reading in the data.

The data is stored in the following order:

```kroki type=rackdiag alt=fileoverviewrack
rackdiag {
  16U;
  ascending;

  1: Header;
  2: Upsert pages PageIds
  3: Upsert pages FileIds
  4: Upsert pages Offset In File
  5: Upsert pages Size
  6: Upsert pages crc32
  7: Deleted pageIds
  8: Upsert file FileIds
  9: Upsert file Page Count
  10: Upsert file non active count
  11: Upsert file size
  12: Upsert file deleted size
  13: Upsert file added at version
  14: Upsert file crc64
  15: Deleted fileIds
  16: Deleted file at version
}

```

### Checkpoint snapshot file

If a checkpoint file ends with ".snapshot.checkpoint" it means that it is a snapshot checkpoint. A snapshot contains all active page-to-file mappings, all active file information, and all pending deleted file entries.

During recovery, the engine finds the latest snapshot and only reads incremental checkpoints from that point forward. This bounds recovery time regardless of how many total checkpoints exist. Snapshots are created every `SnapshotCheckpointInterval` checkpoints (default 20).

### Header layout

The header of the checkpoint file contains offsets to all different arrays of data.
The size of the header is 128 bytes.

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72
  0-31: Magic number
  32-47: Version (2 bytes)
  48-63: Reserved (2 bytes)
  64-127: Updated page count
  128-191: Deleted page count
  192-255: Updated file count
  256-319: Deleted file count
  320-383: Updated pageids start offset
  384-447: Updated page file identifiers start offset
  448-511: Updated page file offset start offset
  512-575: Updated page sizes offset
  576-639: Updated page crc32s offset
  640-703: Deleted pageids start offset

  704-767: Updated files fileIds start offset
  768-831: Updated files page count start offset
  832-895: Updated files non active count start offset
  896-959: Updated files sizes offset
  960-1023: Updated files deleted sizes offset
  1024-1087: Updated files added at version offset
  1088-1151: Updated files crc64s offset
  1152-1215: Deleted files file Ids offset
  1216-1279: Deleted files added at version offset
  1280-1343: Next available file id
}
```

### Changed pages layout

The following layout shows how the three columns for new/updated pages are written to disk and the deleted pages array.

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-63: Page Id 1
  64-127: Page Id 2
  128-191: ...
  192-255: Page Id N
  256-319: File identifier 1
  320-383: File Identifier 2
  384-447: ...
  448-511: File Identifier N
  512-543: Page file offset 1
  544-575: Page file offset 2
  576-607: ...
  608-639: Page file offset N
  640-671: Page size 1
  672-703: Page size 2
  704-735: ...
  736-767: Page size N
  768-799: Page crc32 1
  800-831: Page crc32 2
  832-863: ...
  864-895: Page crc32 N
  896-959: Deleted page 1
  960-1023: Deleted page 2
  1024-1087: ...
  1088-1151: Deleted page N
}
```

### Changed files layout

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-63: File Id 1
  64-127: File Id 2
  128-191: ...
  192-255: File Id N
  256-287: File page count 1
  288-319: File page count 2
  320-351: ...
  352-383: File page count N
  384-415: File non active count 1
  416-447: File non active count 2
  448-479: ...
  480-511: File non active count N
  512-543: Roaring bitmap offset 1
  544-575: Roaring bitmap offset 2
  576-607: ...
  608-639: Roaring bitmap offset N
  640-703: Roaring bitmaps data [colheight = 2]
  768-831: Deleted file id 1
  832-895: Deleted file id 2
  896-959: ...
  960-1023: Deleted file id N
}
```

## Data File

The data files store the actual page data. Each file is a self-contained unit that contains a header, a list of all page ids stored in the file, their byte offsets, and the page data itself.

The data file is structured for streaming reads. During compaction, data is read sequentially from an old file and copied into a new file without needing to seek. The page ids are placed before the page data so that a reader can determine which pages are still active before reading any page content.

The file layout is: a fixed 64-byte header, followed by page ids stored as a column, then page offsets/locations, and finally the page data.


```kroki type=rackdiag alt=fileoverviewrack
rackdiag {
  7U;
  ascending;

  1: Header;
  2: Page Ids
  3: Page Locations
  4: Page Data 1
  5: Page Data 2
  6: ...
  7: Page Data N
}

```

### Header layout

The header of the data file has the following layout:

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-31: Magic number
  32-47: Version
  48-63: Flags
  64-95: Page Counts
  96-127: Page Id Start Offset
  128-159: Page Location Start Offset
  160-191: Data Start Offset
  192-255: Reserved 40 bytes (64 byte alignment)  [colheight = 5]
}
```

The following flags are currently defined:

1. `ContainsCheckpointBundle` (bit index 0): When set, the file is a bundle file that contains page data, checkpoint data, and registry data in a single file. The reserved bytes in the header are repurposed to provide offsets into the bundled sections:

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-63: Checkpoint Offset
  64-127: Registry Offset
  128-191: Registry footer offset
}
```

### Page ids and offset layout

After the header comes the page ids and offsets. These are primarily used during compaction: the page ids are read to determine which pages are still active, and the offsets are used to locate the page data for copying into a new file.

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-63: Page Id 1
  64-127: Page Id 2
  128-191: ...
  192-255: Page Id N
  256-287: Page 1 Offset
  288-319: Page 2 Offset
  320-351: ...
  352-383: Page N Offset
}
```

### Page data

Page data can be written in compact mode with no padding, or with padding to disk block size (512/4096 bytes) to enable aligned reads from local disk. Aligned writes are particularly useful for DIRECT_IO on Linux where reads must be block-aligned.

Since all reads are offset-based (the checkpoint file stores exact byte offsets and sizes for each page), the choice between compact and padded mode is transparent to the reader.


```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-63: Page 1 Data [colheight = 2]
  128-191: Page 2 Data [colheight = 2]
  256-319: ... [colheight = 2]
  384-447: Page N Data [colheight = 2]
}
```

## Checkpoint Registry File

The checkpoint registry file provides a directory of all checkpoint versions that exist in storage. On storage systems that do not support efficient file listing, this file eliminates the need for a list operation during initialization.

The registry contains:
* The version number of each checkpoint.
* A bitmap indicating which checkpoints are snapshots.
* A bitmap indicating which checkpoints are bundled into data files.
* The CRC64 value of each checkpoint, used to verify integrity before reading checkpoint data.

The registry also has a footer that contains the computed CRC64 of the entire registry file (excluding the footer itself). This allows the engine to detect corruption in the registry on read.

On storage providers that support file listing (`SupportsFileListing`), the registry is bundled into checkpoint files instead of being written as a separate file. In this case, the engine reads the registry from the latest checkpoint or bundle file during recovery.

File layout:

```kroki type=rackdiag alt=fileoverviewrack
rackdiag {
  6U;
  ascending;

  1: Header;
  2: Checkpoint Versions
  3: Is snaphots
  4: IsBundle
  5: CRC64s
  6: Footer (crc64 of the registry)
}
```

### Header layout

The header of the checkpoint registry has the following layout:

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-31: Magic number
  32-47: Version
  48-63: Reserved
  64-95: Versions count
  96-127: Checkpoint versions offset
  128-159: Is Snapshots offset
  160-191: CRC64s Offset
  192-223: Footer offset
  224-255: Reserved
  256-319: Reserved 32 bytes (64 byte alignment)  [colheight = 5]
}
```

### Data layout

The data contains all the version numbers of the checkpoints in the registry in order.
It also contains a bitmap that tells if a checkpoint is a snapshot checkpoint or not. This is stored as
a multiple of four bytes, so if there are 5 checkpoints, it will still use four bytes to store the bitlist.
68 checkpoints will use 8 bytes.

Finally it also contains a list of the CRC64 values of the checkpoint files which can be used to check
data integrity of the actual checkpoint files.

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-63: Checkpoint version 1
  64-127: Checkpoint version 2
  128-191: ...
  192-255: Checkpoint version N
  256-319: IsSnapshot Bitmap list  [colheight = 2]
  384-447: CRC64 checkpoint 1
  448-511: CRC64 checkpoint 2
  512-575: ...
  576-639: CRC64 checkpoint N
}
```

### Footer

The footer contains the CRC64 value of the registry file, which allows checking data integrity when reading
the registry.

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-63: Registry CRC64 value
}
```

## Data And Checkpoint Bundle File

The *data and checkpoint bundle file* is a special case file where page data, checkpoint metadata, and registry data are bundled into a single file. This is done to reduce cloud costs on `PUT` operations.

This file is only created when all updated pages for a checkpoint fit within a single file (i.e., no separate data files were written during the checkpoint). It uses a special file id with bit 63 set, encoding the checkpoint version in the lower bits (`1UL << 63 | checkpointVersion`).

Bundle files are not compressed, since the CRC64 values are recalculated across all three sections during creation and compression would add complexity to the chain of integrity checks.

**Cost comparison:**

Over twenty checkpoints, if all changed data can fit under the max file size (e.g., 64 MB):

| Approach | Writes | Deletes (compaction) | Deletes (checkpoint cleanup) | Total operations |
|---|---|---|---|---|
| Separate data + checkpoint files | 40 | 20 | 20 | 80 |
| Bundle files | 20 | 20 | 0 | 40 |

This results in **50% fewer operations** to cloud storage.

The downside of this approach is that it requires a list operation to discover bundle files during recovery. On storage providers where listing is expensive, this model can become more costly than writing separate files if the stream crashes frequently.

### Data Layout

The three sections are concatenated sequentially. The data file header's `ContainsCheckpointBundle` flag is set, and the reserved header bytes store the byte offsets to the checkpoint and registry sections.

```kroki type=rackdiag alt=fileoverviewrack
rackdiag {
  3U;
  ascending;

  1: Page Data;
  2: Checkpoint Data;
  3: Registry Data;
}
```

### CRC64 for bundle files

All three parts have their own CRC64 value, and they are recalculated in order when the bundle is created:

1. The page data CRC64 is computed over all bytes from byte 0 to the start of the checkpoint data.
2. This CRC64 is written into the checkpoint's file information for the bundle. The checkpoint data CRC64 is then recalculated.
3. The updated checkpoint CRC64 is written into the registry. The registry CRC64 is then recalculated and written into the registry footer.

This creates a chain of integrity: verifying the registry CRC64 transitively verifies the checkpoint, which transitively verifies the page data.