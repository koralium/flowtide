---
sidebar_position: 0
---

# Object Structure

## Checkpoint file

The checkpoint file contains the following information:

* **UpdatedPageCount** - How many pages that have updated in the checkpoint
* **DeletedPageCount** - How many pages that have been deleted in the checkpoint
* **UpdatedFileCount** - How many files that have been updated
* **DeletedFileCount** - How many files that have been deleted
* Three columns for updated pages:
  * **PageId** - the identifier of the page that was updated
  * **FileId** - the file identifier that the page resides in the checkpoint
  * **OffsetInFile** - the byte offset where the page can be read from in the file
* **DeletedPageIds** - Array of page ids that have been deleted
* Four columns for new/updated files (updates are only to invalidate replaced page data):
  * **FileIds** - the file identifiers of the files that have been updated.
  * **PageCount** - how many pages that exist in the file.
  * **NonActiveCount** - how many pages that have been invalidated in the file.
  * **RoaringBitmaps** - bitmaps that mark which pages that have been invalidated in a file.
* **DeletedFileIds** - an array of file ids that are no longer active.

The data is stored in the following order:

```kroki type=rackdiag alt=fileoverviewrack
rackdiag {
  10U;
  ascending;

  1: Header;
  2: Upsert pages PageIds
  3: Upsert pages FileIds
  4: Upsert pages Offset In File
  5: Deleted pageIds
  6: Upsert file FileIds
  7: Upsert file Page Count
  8: Upsert file non active count
  9: Upsert file roaring bitmaps
  10: Deleted fileIds
}

```

### Checkpoint snapshot file

If a checkpoint file ends with ".snapshot.bin" it means that it contains all active page information from previous checkpoints.
The snapshot files allows quicker initialization than reading all the checkpoint files from the beginning.

### Header layout

The header of the checkpoint file contains offsets to all different arrays of data.
The size of the header is 128 bytes.

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-15: Version (2 bytes)
  16-63: Reserved (6 bytes)
  64-127: Updated page count
  128-191: Deleted page count
  192-255: Updated file count
  256-319: Deleted file count
  320-383: Updated pageids start offset
  384-447: Updated page file identifiers start offset
  448-511: Updated page file offset start offset
  512-575: Deleted pageids start offset
  576-639: Updated files fileIds start offset
  640-703: Updated files page count start offset
  704-767: Updated files non active count start offset
  768-831: Updated files bitmaps offset array start offset
  832-895: Updated files bitmaps data offset
  896-959: Deleted files fileIds start offset
  960-1023: Padding (8 bytes)
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
  512-575: Page file offset 1
  576-639: Page file offset 2
  640-703: ...
  704-767: Page file offset N
  768-831: Deleted page 1
  832-895: Deleted page 2
  896-959: ...
  960-1023: Deleted page N
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

First the header is stored at a static 64 bytes. After the header is all the data pages.
These are located before page ids and page locations to easily get the page location.
It is important to be able to read a page without reading the entire content of the file from checkpoint information.
Therefore the page ids and locations come last. They are only used when shuffling data from a file into a new file
when a majority of pages are no longer active in the file.

```kroki type=rackdiag alt=fileoverviewrack
rackdiag {
  7U;
  ascending;

  1: Header;
  2: Page Data 1
  3: Page Data 2
  4: ...
  5: Page Data N
  6: Page Ids
  7: Page Locations
}

```

### Header layout

The header of the data file has the following layout:

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-15: Version
  16-31: Reserved
  32-63: Page Counts
  64-95: Page Id Start Offset
  96-127: Page Location Start Offset
  128-159: Data Start Offset
  160-191: Reserved
  192-255: Reserved 56 bytes (64 byte alignment)  [colheight = 5]
}
```

### Page data

Page data can be written in compact mode with no padding, or padding to disk block size (4096 bytes) to
make it quicker to read data from local disk.

Since its offset dependent this is up to the implementation.

The page data should be padded to the next 64 byte alignment at the end for page ids and offsets to
allow avx iteration of page ids.

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-63: Page 1 Data [colheight = 2]
  128-191: Page 2 Data [colheight = 2]
  256-319: ... [colheight = 2]
  384-447: Page N Data [colheight = 2]
  512-575: Padding (to next 64 byte alignment)
}
```

### Page ids and offset layout

After the data comes the page ids and offsets, these are used mainly if a file should be merged into a new file
if too many pages have been invalidated inside of a file. This allows iteration of the roaring bitmap with the page ids
and offsets to write into a new file.

```kroki type=packetdiag
{
  colwidth = 64
  node_height = 72

  0-63: Page Id 1
  64-127: Page Id 2
  128-191: ...
  192-255: Page Id N
  256-319: Padding to next 64 byte alignment
  320-351: Page 1 Offset
  352-383: Page 2 Offset
  384-415: ...
  416-447: Page N Offset
}
```