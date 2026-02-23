---
sidebar_position: 0
---

# Object Structure

## Storage Hierarchy

Flowtide utilizes a 5-tier storage model to balance low latency with elastic cloud scalability:
1. RAM Pages (LRU): Hot, mutable state for active operator processing.
2. Disk Spillover: Local mutable storage for active checkpoints that exceed RAM capacity.
3. In-Flight Buffer: Immutable RAM buffer that holds data during the commit process, ensuring Read-Your-Own-Writes consistency before disk confirmation.
4. Local Disk Cache: Immutable, CRC-verified cache of 64MB blobs fetched on-demand from remote storage.
5. Persistent Storage: The definitive source of truth, typically hosted on Amazon S3 or Azure Blob Storage.

This document focuses on enabling tier 3 (In-Flight Buffer), 4 (Local Disk Cache) and 5 (Persistent Storage).

## In-Flight Buffer

The **In-Flight Buffer** is a critical volatile storage layer that bridges the gap between operator data production and persistent storage confirmation. It ensures **Read-Your-Own-Writes (RYOW)** consistency and provides backpressure during heavy write loads.

When an operator "commits" data, it is transitioned from a mutable state to an immutable state. Before this data is successfully written to the persistent storage, it resides in the **In-Flight Buffer**.

Key functions:

*   **Zero-Downtime Reads:** Allows other operators to read newly produced data immediately, even if the background upload to S3/Azure is still in progress.
*   **Data Integrity:** Holds the immutable bytes used for the final **CRC64** calculation.
*   **Backpressure:** Uses a `BoundedChannel` to limit the number of pending writes, preventing the engine from exhausting RAM if the storage backend is slow.

### Internal workflow

1. When an operator commits its state to storage, the memory is copied into file buffers for pending persistent storage writes.
2. An interal temporary lookup dictionary is used to connect page identifier to the memory inside of the file buffer.
   If a page is read from the file buffer, the memory is copied into a mutable memory area to ensure integrity of the memory.
3. The file buffers are enqueued to a bounded channel and are picked up by persistent storage write tasks.
4. When a file has been written to storage, the pages are removed from the lookup dictionary.

### On Stream Failure

On stream failure, the entire in flight buffer is cleared of data, since no checkpoint has been taken.

## Local Disk Cache

The **Local Disk Cache** is an immutable, on-demand storage layer that enables Flowtide to achieve near-local SSD performance while maintaining the state in cloud storage (S3/Azure). 

The cache acts as a "Warm" tier for 64MB blobs. Instead of reading directly from the cloud for every page miss, Flowtide fetches entire blobs, verifies them, and serves subsequent page requests directly from the local disk.

Key features:

*   **On-Demand Fetching:** Blobs are only downloaded when a specific `PageId` is requested by an operator.
*   **Self-Healing:** If a local page read fails a CRC check, the cache automatically purges the local file and re-downloads a fresh copy from the cloud.
*   **DIRECT_IO Support:** Optimized for Docker/Linux environments to bypass the OS Page Cache, preventing double-buffering and OOM kills.

### Internal workflow

* **Cache misses** 
  1. When a page is requested on a file that does not exist in the cache, the file is downloaded from persistent storage.
  2. The file is verified against the CRC64 value stored in the checkpoint files to ensure integrity.
  3. The file is stored on local disk to allow quick page fetching based on offsets and to reduce cloud API calls.
* **Fetching a page from the cache**
  1. When a page is requested from the cache, it reads from the specified offset and length in the file the page resides in.
  2. The page data is is verified against the stored CRC32 value of the page data to ensure data integrity.
  3. The page data is returned to the caller.
* **New data file**
  1. When a new data file is written to the persistent storage, the file is also written to the cache. This
     reduces latency for newly written files.

### On Stream Failure

On stream failure, the local disk cache is not cleared, since files are immutable they can be from previous checkpoints
and this will help reduce the amount of API calls to remote storage.


## Persistent Storage Layer

The **Persistent Storage Layer** is the final storage tier to ensure persistence. It provides long-term, durable storage for all streaming states, typically hosted in object stores like [Amazon S3](https://aws.amazon.com) or [Azure Blob Storage](https://azure.microsoft.com).

This layer is designed to be "Cloud-Native," treating remote storage as an append-only log of immutable 64MB blobs. 

Key features:

*   **Immutable Blobs:** Data is packed into files to saturate network bandwidth and minimize cloud API costs (PUT/GET).
*   **Checkpoint files:** Checkpoint files with Page mapping, offsets, and CRCs enables quick restore times.
*   **Checkpoint Registry (`checkpoints.registry`):** A "pointer" file that tells the engine which checkpoint versions exist.
*   **CRC64 Verification:** Every file is hashed using CRC64 to guarantee integrity across different cloud providers.

## Checkpoint file

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

If a checkpoint file ends with ".snapshot.checkpoint" it means that it contains all active page information from previous checkpoints.
The snapshot files allows quicker initialization than reading all the checkpoint files from the beginning.

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

## Data file

The data files store the page data.
Each file contains information of all the page ids
and the offset from the start of the file including header.

The data file is built to allow streaming reads which is useful
during compaction processes where data is copied from an old file into a new file.

First the header is stored at a static 64 bytes, after that comes the page ids stored as a column. This allows reading only pageIds
to check if any page in the file is still active.
After the pageIds comes the offsets/locations of the data pages. Finally the data pages.


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
  48-63: Reserved
  64-95: Page Counts
  96-127: Page Id Start Offset
  128-159: Page Location Start Offset
  160-191: Data Start Offset
  192-255: Reserved 40 bytes (64 byte alignment)  [colheight = 5]
}
```

### Page ids and offset layout

After the header comes the page ids and offsets, these are used mainly if a file should be merged into a new file
if too many pages have been invalidated inside of a file.

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

Page data can be written in compact mode with no padding, or padding to disk block size (512/4096 bytes) to
make it quicker to read data from local disk.

Since its offset dependent this is up to the implementation.


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

The checkpoint registry file serves the single purpose
of providing which checkpoint version files exist.

This file allows skipping a list operation on storage systems
when initializing which may be costly or difficult on certain
storage backends.

It also contains the CRC64 values of the checkpoint files which
is used to check the data integrity of the checkpoint files.

The registry also comes with a footer that contains the computed CRC64 of the registry file.
This is without the actual footer itself.

File layout:

```kroki type=rackdiag alt=fileoverviewrack
rackdiag {
  5U;
  ascending;

  1: Header;
  2: Checkpoint Versions
  3: Is snaphots
  4: CRC64s
  5: Footer (crc64 of the registry)
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