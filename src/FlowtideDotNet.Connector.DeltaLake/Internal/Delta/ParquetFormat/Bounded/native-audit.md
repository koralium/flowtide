# Native allocation audit

Pinned: ParquetSharp 23.0.0.2 (Arrow C++ 23.0.0). Measurements: META, ENV and ENV-W of 2026-10-11, fresh processes, net10.0 Release, i7-13700H, Windows 11.

Every constant in `NativeConstants` has one row. A row is **resolved** only with a complete upper-bound derivation over admitted inputs, with file:line in the pinned sources, covering retained capacity and growth overlap. No row is resolved yet: the Arrow sources (`buffered.cc`, `column_reader.cc`, `metadata.cc`, `file_reader.cc`, `file_writer.cc`, `column_writer.cc` and `memory_pool.cc`) were read through summaries, so the derivations below name the construction path but cannot cite exact lines or enumerate every allocation. Unresolved rows are usable only once accepted; all are accepted, see the decisions below.

| Id | Term | Construction path | Derivation | Fit | Reserved | Status |
|---|---|---|---|---|---|---|
| ChunkMetadata | c_chunk, per column chunk of every column, projected or not | `metadata.cc` deserializes the whole Thrift `FileMetaData`; `format::ColumnChunk` with `ColumnMetaData`, `Statistics`, encodings and `path_in_schema` per chunk | Struct sizes not taken from the pinned headers; per-chunk strings and lists are charged separately | 950 B (least squares over 9 META footers, 10 to 10,000 chunks) | 1,536 B | Unresolved, accepted |
| SchemaElementMetadata | c_schema, per schema element | Thrift `SchemaElement`, plus the `schema::Node` tree and one `ColumnDescriptor` per leaf built by `SchemaDescriptor` | Not derived | 1,100 B (200-leaf footer against the 10-leaf one); the provisional 512 B was falsified and raised to at least 1.5 × the fit | 1,792 B | Unresolved, accepted |
| RowGroupMetadata | c_rg, per row group | Thrift `RowGroup` and its `RowGroupMetaData` wrapper | Not derived | 331 B | 1,024 B | Unresolved, accepted |
| StringFactor | Coefficient on footer string and binary bytes | Thrift strings are copied out of the footer buffer; key-value pairs are copied again into `KeyValueMetadata` | Not derived beyond the two copies named | 1.0 per string byte beyond F (stats and key-value footers) | 2 | Unresolved, accepted |
| ListEntryBytes | Coefficient on footer list entries | Thrift `std::vector` elements (encodings, path parts) | Not derived | Absorbed in the chunk fit | 4 B | Unresolved, accepted |
| CodecPerLeaf | c_codec, decompressor state per leaf | Arrow `Codec` per column reader: zstd `ZSTD_decompress` context, zlib inflate state and 32 KiB window, Snappy and LZ4 scratch | Per-codec state not derived from the bundled codec sources | Not isolated; ENV totals stay below 3 % of the reserved D(P) | 1 MiB | Unresolved, accepted |
| ReaderFixed | C_fixed, reader objects | `ParquetFileReader`, `RowGroupReader`, column readers, buffered streams | Not derived | 1.03 MB fixed cost per open (META intercept) | 16 MiB | Unresolved, accepted |
| HeaderBound | H_L for our own output | Heap `PageHeader` plus the `Advance` copy of the header | Our headers carry no statistics | Under 100 B per header | 2 KiB | Unresolved, accepted |
| PoolPerLeaf | Pool allocations of a column reader outside the page terms | Not identified | Not derived | About 50 B per leaf (2.5 KB over 52 leaves, adversarial tiny pages) | 1 KiB | Unresolved, accepted |
| FooterReadSize | The speculative footer read | `file_reader.cc` reads the last 64 KiB before parsing | A fixed read size, not a budget; not checked against the pinned line | Not fitted: the 64,512 B pool peak on the golden roots only confirms the read size | 64 KiB | Unresolved, accepted |
| WriterChunkMetadata | c_chunk_w, metadata kept per written chunk until close | `file_writer.cc` row-group and column-chunk metadata builders | Not derived | Not isolated | 1,536 B | Unresolved, accepted |
| WriterCodec | c_codec_w, compressor state | Arrow `Codec::Compress` per page | Not derived. The compressed-size terms use each codec's worst case; gzip takes zlib's conservative `deflateBound` (about 1.14 n), since the level may reach zlib as another parameter, not checked in the pinned source | Not isolated; ENV-W pool peaks at most 39 % of the projected W_out | 3 MiB | Unresolved, accepted |
| WriterFixed | C_fixed_w, writer objects | `FileWriter`, column writers, sinks | Not derived | Not isolated | 8 MiB | Unresolved, accepted |
| OwnFooterBytesPerChunk | Footer bytes per chunk of our output, for M(own) | Our writer's footer | Estimate | Not measured | 160 B | Unresolved, accepted |
| OwnFooterStringBytesPerChunk | Footer string bytes per chunk of our output | Our writer's footer | Estimate | Not measured | 40 B | Unresolved, accepted |
| OwnFooterListEntriesPerChunk | Footer list entries per chunk of our output | Our writer's footer | Estimate | Not measured | 6 | Unresolved, accepted |
| OwnFooterFixed | Footer bytes of our output outside the chunks | Schema, key-value pairs with `ARROW:schema` and the marker | Estimate | Not measured | 64 KiB | Unresolved, accepted |

## Formula terms

The formulas as implemented; they rest on the source reads summarized above.

- **D(P)** per row group: Σ over projected leaves of (S_L + U_L + Dict_L + 2·H_L + c_codec) + C_g + Ω_g, maximum over groups, plus C_fixed. S_L comes from the replayed `BufferedInputStream` (`StreamReplay`), which grows to the peek's end whenever a header peek runs past the buffer, so a chunk of many small pages grows it toward the whole chunk.
- **Ω_g**: the largest of S_L, U_L and Dict_L in the group.
- **M(P)**: the reader's footer metadata, the footer length plus ChunkMetadata per column chunk, SchemaElementMetadata per schema element, RowGroupMetadata per row group, StringFactor per footer string byte and ListEntryBytes per footer list entry.
- **Pool-visible part**: S + U + Dict + one header copy + C_g + PoolPerLeaf per leaf, or the footer read when larger.
- **W_out** and **Ω_W**: the writer's projected native memory and its growth overlap, computed by `CheckpointFileWriter.ProjectedWriter`.

## Measurements against the checks

- **ENV** (8 roots: legacy at 200k and 1M files, three adversarial roots, three golden roots): private bytes above the warm baseline stay within D(P) + M(P) + the managed peak + 16 MiB on every root, with the largest at 22 MB against 110 MB. Throughput is 0.90× to 15× the Arrow reader. The pool peak stays within the pool-visible term once the footer read and the per-leaf term are counted.
- **META** (9 footers, 846 B to 8.3 MB): at and above 4,000 chunks the native cost stays within M(P). Small footers carry a fixed open cost of about 0.6 to 1.0 MB that M(P) does not model; C_fixed covers it.
- **ENV-W** (3 inputs × 6 codecs): the pool peak stays within the projected W_out in all 18 runs, at most 39 % of it. Private bytes exceed the projection in one run, the 1M-file content uncompressed (227 MB against 110 MB). The cause is outside the native terms: ParquetSharp's net8.0 `ManagedOutputStream` allocates a managed array per write, about 1.13 × the output bytes (230 MB of managed allocations for 203 MB of output).

## Decisions (2026-10-11)

- **Every row above is accepted at its reserved value.** Each reservation is at least 1.5 × its fit where a fit exists; the ENV and ENV-W checks remain the falsifiers. `ReservedValuesAreAtLeastTheirDerivedBounds` passes only for accepted rows.
- **The writer writes its scratch file natively.** `ParquetSharp.Arrow.FileWriter(path, ...)` replaces the managed stream, which removes the managed copy of every write. Reserve-before-write holds at group granularity: before a row group is flushed, the disk ledger reserves its projected bytes (the codec's worst case over values and levels, plus 1 KiB per page). Before the footer, it reserves 1 MiB plus 1 KiB per chunk. At close the file is flushed with `Flush(true)` and the charge reconciled to its length.

## Compaction terms (Phase 6 PR 2, 2026-11-07)

Measured on Linux (Ubuntu 24.04, .NET 10.0.14, 4 vCPU), Release, system pool, ParquetSharp 23.0.0.2. These terms live in `DataFileAdmission` and `CompactionCopy`, not `NativeConstants`, so they are listed here rather than in the table.

**Probe.** `compaction-audit` (tests `CompactionAudit.cs`) copies one bin per fresh process, after a tiny warm-up bin. Generated inputs are written by an earlier process and read from a manifest.
- The gate is the whole process: RSS above the start, sampled every millisecond, against the ledger's native and managed peaks together, so nothing is subtracted. On Linux `PrivateMemorySize64` is VmData, which counts untouched writable mappings (257 MB in a test process holding 52 MB resident anonymous, no swap), so RSS, a superset of resident private memory, stands in for private bytes. As in the Windows audit above, the formula it is held to includes the managed peak.
- Native alone, RSS less the GC's growth against the native peak, gates at 0.9 too, so a native miss cannot hide under managed headroom. It is the weaker reading: subtracting committed managed growth can under-measure native when the GC commits pages it has not touched.
- Live managed bytes come from a full collection at each input and at close, against the ledger at that moment. That misses the builders' peak inside an input, which the growth reservation covers.
- Each output is then admitted again alone at its target.

**Fixtures.** 8, 16, 32 and 64 MiB, zstd and Snappy (64 MiB left the validated set after this audit, see below), over these inputs.
- Eighteen generated shapes: narrow; six strings; six strings in 8 MiB groups; Pareto-tailed strings; 64 nullable booleans; deep nullable structs with a list of structs and a map; 16 distinct 4 KiB strings; one 8 MiB value per file; groups of 10 rows; skewed groups; a 41-field struct missing under a list and beside it; decimal(38,4) and decimal(9,2); one-row lists of 200,000 longs; lists of 0 to 50 longs; lists of 0 to 50 int8 in 20,000-row and 2,000-row files; lists of 5 and 25 booleans.
- The sink fixtures K′, U′ and Krand¼, first bin of 50 live small files.
- The first matrix ran twice, with and without the full collections; the last one, over the final code at 8, 16 and 32 MiB, ran with them.

**Results.**
- Whole process (the gate): RSS at most 0.57 of the native and managed peaks together in every run. The 0.9 rule holds.
- Native alone: at most 0.60 of the ledger's native peak over every run (0.54 in the last matrix, median 0.25).
- Managed: live bytes stayed under the ledger at every point, the closest 3.1 MiB under, and at most 0.55 of the managed peak.
- Re-admission at 8, 16 and 32 MiB: every ordinary and list output, and K′, U′ and Krand¼, at B = 65,536; string-heavy 8 MiB groups at 8,192. The gate's floor is the inputs' smallest batch when that is under 1,024 (user ruling), which the dictionary blow-up meets at 64; one 8 MiB value per file at 32 MiB with zstd declines and is a known miss. At 64 MiB string tables fell to B = 1 to 256, so 64 MiB left the validated set. That was under F = T/4; the next block caps F.

**Targets above 32 MiB (2026-11-08).** PR 2 now supports larger targets. Which targets users may pick is decided with PR 4 and PR 5.
- F = min(8 MiB, max(1 MiB, T/4)). Above 32 MiB a larger target only adds groups, so Cap, Λ and W_group stop growing.
- Before each batch, a group holding at least F/2 is flushed if the batch would take it past 12 MiB. The batch is expected to grow at the larger of the open group's and its own input group's bytes per row, capped by its bound. Size statistics make the group's mean exact, so a wider input or group is seen before it lands. A batch skewed within its group can still pass the estimate. The group then reaches F plus one batch and can re-admit below the floor, as the approved base does at 32 MiB (wide rows before empty ones in one group re-admitted at B = 2 there). The user accepted this as a known limit; a per-batch bound from page sizes is left for a later PR. Files without size statistics bound a dictionary chunk at values × page, so each batch of such an input is judged by its bound, and its groups can close as early as F/2.
  - Without this, a whole input group landed on a full group: strings at 128 MiB Snappy re-admitted at B = 2, and string-groups at 8,192.
  - Judging by the bound alone cut groups too often. A dictionary value is bounded by its page, and any batch smaller than its group by the whole chunk. A ceiling of 1.5 F also split one-row lists at 8 MiB. The 64-group stop then copied fewer inputs (dictionary 30 to 15 at 32 MiB), or outputs grew (long lists, 9%).
  - The ceiling is fixed, because re-admission depends on a group's absolute size against the managed cap. Groups of up to about 11 MiB of builder bytes re-admitted at 65,536 in every run, and F plus a whole 15 MB input group re-admitted at 2.
- glibc kept freed builder and page buffers resident.
  - Untrimmed, decimal at 128 MiB measured 0.785 to 0.939 of the native peak over 16 runs, and up to 1.03 with `MALLOC_ARENA_MAX=1`. With large allocations kept on mmap it measured 0.44.
  - Each flushed group now calls `malloc_trim` where glibc allocates (`NativeHeap`), which brings decimal to 0.62 to 0.67 over 11 runs.
  - It costs 5 to 14% of copy time at 128 MiB on the heaviest shapes (decimal 9%, narrow 5% and 14% in two rounds), and nothing measurable on K′.
  - Windows was not remeasured. `malloc_trim` returns free pages across the whole process, so the sink's own latency with compaction on is for PR 4 and PR 5 to measure.
- Matrix over the final code, 115 bins: 8, 16 and 32 MiB with zstd, 128 MiB with both codecs, and U′, narrow, nested, strings and string-groups at 64 MiB. The versions before it ran 8, 16, 32 and 128 MiB with both codecs.
  - Memory, over the 111 bins with a native reservation: native at most 0.642 (median 0.245), and the whole process at most 0.634. A run of the version before reached 0.71 for the whole process on a retried bin's first attempt. Live managed bytes stayed at least 3 MiB under the ledger.
  - Re-admission: every ordinary and list output, and K′, U′ and Krand¼, at B = 65,536. The dictionary blow-up re-admits at 64, its inputs' batch, at 32 and 128 MiB. One 8 MiB value per file with zstd stays the known miss, at 128 MiB too.
  - Against the approved base, with zstd: at 8 and 16 MiB every bin copies the same inputs into an output of the same size, with the same re-admission. At 32 MiB every bin does too, except string-groups, which now re-admits at 65,536 instead of 8,192, with an output 1% larger, a size trade the user accepted.
  - One malloc arena: the six 128 MiB bins with the highest native ratios on an earlier version stay at most 0.67. Its 128 MiB outputs had the same inputs, sizes and re-admission as the final code's.
- At 128 MiB three shapes stop early by design.
  - Lists of 5 booleans hit the 10M-row cap at 3 inputs, 10 to 12 MiB.
  - 41 null strings under a list hit the 64-group stop at 4 inputs, 64 to 75 MiB. Each null string costs a 4-byte offset in Arrow and almost nothing in parquet.
  - 16 distinct 4 KiB strings hit it at 30 inputs, as at 32 MiB, since each group carries its dictionary.

**Terms.**
- Batch_g(B) and RecordReader_g(B): A10, at most 0.85 of the whole formula.
- M_i adds the 64 KiB speculative read when the footer is longer, since Arrow copies the footer beside it. A10's open peak was that read plus the footer plus 8 B.
- Built, Cap = 2(F + Out_max) + 64 B per buffer, the growth reservation, W_group and the dictionary memo (16 or 24 B per slot, growing ×4 past half full with the old table live): derived from the Arrow 23 sources in M2, measured here.
- The level cap Λ per list leaf, max(the largest input group's level slots, ⌈F·ρ/4⌉, 2^20): the copier closes a group before an input group would pass it, and W_group counts at most Λ levels and, for fixed and boolean leaves, the bytes Λ slots can build.
- The output's disk: the output's cap before pass 1, then before each group the file so far, the previous group (Arrow writes its last column chunk with the next group), this group at the codec's worst case with a dictionary as large again and 1 KiB per page, every page's index entry (min and max at up to 4 KiB each), and a footer of 1 MiB plus 9 KiB per chunk.
- StatisticsPerLeaf, 6 KiB (512 B provisional): the writer's objects, its statistics and the stats JSON, beside the two values (2 × largest, kept). A throwaway measurement gave 688 B per int64 leaf and 3,717 B per string leaf with non-ASCII bounds, so 6 KiB is 1.65× the larger.
- PrecisionCursor, 64 KiB + 16 KiB (64 KiB provisional): three pooled arrays of 4,096 (definitions, repetitions, INT96 values) and the cursor's objects. The first pass over 30,000 INT96 rows allocates 75,896 B, 3,752 B of it the bitmap, so the objects take 6,608 B.
- VectorTransient, 400 KiB: a run container's decode beside the vector, not isolated here.
