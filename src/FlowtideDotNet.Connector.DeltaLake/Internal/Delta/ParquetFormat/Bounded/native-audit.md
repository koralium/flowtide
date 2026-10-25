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
