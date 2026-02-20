---
sidebar_position: 0
---

# Object Structure

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

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-63: Checkpoint version 1
  64-127: Checkpoint version 2
  128-191: ...
  192-255: Checkpoint version N
  256-287: Page 1 Offset
  288-319: Page 2 Offset
  320-351: ...
  352-383: Page N Offset
}
```