---
sidebar_position: 3
---

# Memory Cache

The purpose of Flowtide's memory cache is to hold as many of the data pages required for the stream in memory as possible.
It serves all data pages for the different B+ trees used by different operators. The cache can evict data not in use
to a temporary surface on disk to reduce RAM pressure.

## Goal

The goals of the memory cache are as follows:

* High hit rate %, it should try and estimate which pages might be reused and keep them in RAM.
* Low performance overhead, fetching pages from the cache should not be a majority factor in performance.
* Low memory usage, the cache should also estimate pages that are probably not required and throw them out early. Meaning do not just grow up to 100% cache utilization if not all pages are estimated to be required by the stream.

## Design

The cache is heavily inspired and based on S3-FIFO in combination with Clock2Q+ with correlation windows.

The cache has three queues just as in S3-FIFO:

* Small queue, for recency.
* Main queue, for frequency.
* Ghost queue for evicted pages.

The small queue starts at 10% of the cache size, main queue 90% and ghost queue 50% of the total size.

When a page gets added it goes into the small queue. A page can get promoted to the main queue if it has at least a frequency of 2. This number was chosen since many operators do at least one fetch even if it might not be reused in the future. The maximum frequency of any page is 3.

To get a frequency increase the page must have travelled 1/40 of the total cache size, measured in cache elements. So in the small queue it must travel 25% of the small queue (being bumped by new elements) before a frequency hit will count. This is inspired by Clock2Q+. So to get bumped to main it must be reused twice but also being moved 25% of the small queue between each hit. This filters out temporarily hot pages from being promoted.

The 1/40 gap is also applied to frequency in the main pool. This is done to also make it harder to gain frequency and to keep pages that are actually reused during a longer timespan.

When a page gets evicted it goes into the ghost queue. This goes for both pages from small and main; this is another difference from S3-FIFO. When being added to the ghost queue, it adds information if the page has at least a frequency of 1. This allows a page to be added directly to the main queue if it had a frequency above 1. If not it gets added to the small queue with a start frequency of 1 to more easily help it promote.

These rules are all to try and help high hit rate percentage, and also low performance overhead since most fetches can be done lock free because of the S3-FIFO adaption.

To try and reduce memory usage an extra rule is added which is to look at the ghost expirations and hit rate to try and right size the small queue.

If many small queue pages travel the entire ghost queue without any reuse at all, the small queue is reduced in size and allows the main queue to grow. If the stream is mostly recency based, this then allows the stream to utilize less memory. The main queue is also aged based on number of inserts into the queue, meaning elements in main will lose frequency even if the main queue is not full. This allows elements to be evicted from main if they have not been used for a long time, to help reduce memory usage.

During testing of a stream which works on recency, such as joining the latest events based on timestamp or similar, this can help heavily to reduce memory usage since usually only the rightmost leaf page in the B+ trees is required to be updated, together with the rightmost parent nodes.
