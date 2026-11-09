// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded
{
    // A data file declined before any native open
    internal sealed class DataFileDeclinedException : Exception
    {
        public DataFileDeclinedException(CompactionReason reason, string message) : base(message)
        {
            Reason = reason;
        }

        public CompactionReason Reason { get; }
    }

    // One read leaf's page terms in one row group
    internal struct LeafTerms
    {
        // S_L, the stream buffer's peak
        public long Stream;
        // U_L, the largest page after decompression
        public long Page;
        // The decompression buffer, the page itself when uncompressed
        public long Buffer;
        // The largest compressed page body
        public long Compressed;
        // Dict_L, the decoded dictionary
        public long Dictionary;
        // The dictionary page's uncompressed bytes
        public long DictionaryPage;
        // H_L, the largest page header
        public long Header;
        // The most values one data page holds
        public long PageValues;
        // n, the chunk's level entries
        public long Values;
        // U, the chunk's uncompressed bytes
        public long Uncompressed;
        // SS, kept for dictionary chunks
        public long? Unencoded;
        public bool HasDictionary;
        public bool IsCompressed;

        // No value is larger than its page or the dictionary page
        public readonly long MaxValue => Math.Max(DictionaryPage, Page);
    }

    // A data file's footer, schema and page terms, read in managed code
    internal sealed class DataFileScan
    {
        private readonly int[] _position;

        public DataFileScan(ParquetFooterModel footer, DataFileSchema schema, int[] readLeaves, LeafTerms[][] groups, long chargedBytes)
        {
            Footer = footer;
            Schema = schema;
            ReadLeaves = readLeaves;
            Groups = groups;
            ChargedBytes = chargedBytes;
            _position = new int[schema.Leaves.Count];
            Array.Fill(_position, -1);
            for (int i = 0; i < readLeaves.Length; i++)
            {
                _position[readLeaves[i]] = i;
            }
        }

        public ParquetFooterModel Footer { get; }

        public DataFileSchema Schema { get; }

        // Column indexes of the leaves read, ascending
        public int[] ReadLeaves { get; }

        // Per row group, per read leaf in ReadLeaves order
        public LeafTerms[][] Groups { get; }

        // The model and the terms table, held in the job's managed ledger
        public long ChargedBytes { get; }

        // Where a column sits in ReadLeaves, or -1
        public int Position(int leaf) => _position[leaf];

        // M_i: the footer, the reader's metadata, and a long footer's first read
        public long Metadata => checked(Footer.FooterLength
            + (Footer.FooterLength + 8 > NativeConstants.FooterReadSize ? NativeConstants.FooterReadSize : 0)
            + NativeConstants.ChunkMetadata * Footer.TotalChunks
            + NativeConstants.SchemaElementMetadata * Footer.Schema.Count
            + NativeConstants.RowGroupMetadata * Footer.RowGroupRows.Count
            + NativeConstants.StringFactor * Footer.StringBytes
            + NativeConstants.ListEntryBytes * Footer.ListEntries);

        // D_g and the reader pool's part of it, for some or all leaves
        public (long Decode, long Pool) Decode(int group, Func<int, bool>? include = null)
        {
            checked
            {
                long terms = 0, pool = 0, largestBody = 0, overlap = 0;
                for (int i = 0; i < ReadLeaves.Length; i++)
                {
                    if (include != null && !include(ReadLeaves[i]))
                    {
                        continue;
                    }
                    var leaf = Groups[group][i];
                    terms += leaf.Stream + leaf.Buffer + leaf.Dictionary + 2 * leaf.Header + NativeConstants.CodecPerLeaf + NativeConstants.PoolPerLeaf;
                    pool += leaf.Stream + leaf.Buffer + leaf.Dictionary + leaf.Header + NativeConstants.PoolPerLeaf;
                    if (leaf.IsCompressed)
                    {
                        largestBody = Math.Max(largestBody, leaf.Compressed);
                    }
                    overlap = Math.Max(overlap, Math.Max(leaf.Stream, Math.Max(leaf.Buffer, leaf.Dictionary)));
                }
                return (terms + largestBody + overlap, pool + largestBody);
            }
        }

        // Batch_g(B), RecordReader_g(B) and the read batch's part under lists
        public (long Batch, long RecordReader, long Repeated) ReadBatch(int group, long batchRows)
        {
            checked
            {
                var rows = Math.Min(batchRows, Footer.RowGroupRows[group]);
                long batch = 0, levels = 0, largestLeaf = 0, repeated = 0;
                foreach (var column in Schema.Columns)
                {
                    batch += Nodes(group, column, rows, ref levels, ref largestLeaf, ref repeated);
                }
                return (batch, levels + largestLeaf, repeated);
            }
        }

        // A node and its read descendants; v is rows outside lists
        private long Nodes(int group, DataNode node, long rows, ref long levels, ref long largestLeaf, ref long repeated)
        {
            checked
            {
                if (!HasReadLeaf(node))
                {
                    return 0;
                }
                var v = node.Repeated ? LargestValues(group, node) : rows;
                if (node.Kind == DataNodeKind.Leaf)
                {
                    // The batch holds v, the leaf being read its capacity c
                    var c = Capacity(group, node, v);
                    var leaf = Slots(group, node, v);
                    // Levels, validity and a copied leaf's values keep their capacity
                    var held = 2 * c * (node.MaxRepetition > 0 ? 2 : 1) + (c + 7) / 8 + (node.Copied ? Physical(group, node, c) : 0);
                    levels += held;
                    // Byte-array and FLBA builders double, so twice the slots
                    var builder = node.Shape == LeafShape.Bytes || node.Element.Type == ParquetPhysicalType.FixedLenByteArray;
                    largestLeaf = Math.Max(largestLeaf, (builder ? 2 : 1) * Slots(group, node, c));
                    repeated += node.Repeated ? leaf + held : 0;
                    return leaf;
                }
                var bytes = (v + 7) / 8 + 64;
                if (node.Kind is DataNodeKind.List or DataNodeKind.Map)
                {
                    bytes += 4 * (v + 1);
                }
                if (node.Kind == DataNodeKind.Map)
                {
                    // The entries struct between the map and its key and value
                    bytes += (LargestValues(group, node) + 7) / 8 + 64;
                }
                repeated += node.Repeated ? bytes : 0;
                foreach (var child in node.Children)
                {
                    bytes += Nodes(group, child, rows, ref levels, ref largestLeaf, ref repeated);
                }
                return bytes;
            }
        }

        // Validity, offsets and values of n slots
        private long Slots(int group, DataNode leaf, long n)
        {
            return checked((n + 7) / 8 + 64 + (leaf.Shape == LeafShape.Bytes ? 4 * (n + 1) : 0) + Values(group, leaf, n));
        }

        // At least 1,024, plus a page's buffered values, to a power of two
        public long Capacity(int group, DataNode leaf, long v)
        {
            var terms = Groups[group][_position[leaf.Leaf]];
            var needed = Math.Max(1024, Math.Min(checked(v + terms.PageValues), Math.Max(v, terms.Values)));
            return checked((long)System.Numerics.BitOperations.RoundUpToPowerOf2((ulong)needed));
        }

        // A copied leaf's reader values; decimals stand for their conversion
        private long Physical(int group, DataNode leaf, long capacity)
        {
            checked
            {
                if (leaf.Element.Type != ParquetPhysicalType.ByteArray)
                {
                    return leaf.PhysicalWidth * capacity;
                }
                var terms = Groups[group][_position[leaf.Leaf]];
                return 4 * (capacity + 1) + Math.Min(Bytes(terms), capacity * terms.MaxValue);
            }
        }

        // vals_ℓ(v): fixed width, bits, or byte arrays bounded by their size statistics
        public long Values(int group, DataNode leaf, long v)
        {
            checked
            {
                if (leaf.Shape == LeafShape.Fixed)
                {
                    return leaf.Width * v;
                }
                if (leaf.Shape == LeafShape.Bits)
                {
                    return (v + 7) / 8;
                }
                var terms = Groups[group][_position[leaf.Leaf]];
                return Math.Min(Bytes(terms), v * terms.MaxValue);
            }
        }

        // Bytes_ℓ: size statistics, else U without a dictionary, else n·maxVal
        public static long Bytes(LeafTerms terms)
        {
            return terms.Unencoded ?? (terms.HasDictionary ? checked(terms.Values * terms.MaxValue) : terms.Uncompressed);
        }

        public bool HasReadLeaf(DataNode node)
        {
            for (int leaf = node.FirstLeaf; leaf < node.FirstLeaf + node.LeafCount; leaf++)
            {
                if (_position[leaf] >= 0)
                {
                    return true;
                }
            }
            return false;
        }

        // The largest n of the read leaves under a node
        public long LargestValues(int group, DataNode node)
        {
            long values = 0;
            for (int leaf = node.FirstLeaf; leaf < node.FirstLeaf + node.LeafCount; leaf++)
            {
                if (_position[leaf] >= 0)
                {
                    values = Math.Max(values, Groups[group][_position[leaf]].Values);
                }
            }
            return values;
        }
    }

    // The managed scan of a data file's footer and read page headers
    internal static class DataPageHeaderScan
    {
        private const int Lzo = 3, Brotli = 4, Lz4Raw = 7;

        // A LeafTerms entry and its array slot
        private const long TermsBytes = 112;

        public static DataFileScan Scan(string path, JobMemory memory, Func<DataFileSchema, bool[]> project, ScanLimits? limits = null)
        {
            limits ??= ScanLimits.Default;
            using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1, FileOptions.RandomAccess);
            var (footerLength, dataEnd) = CheckpointPageScan.ReadFrame(file, limits, (detail, reason) => new DataFileDeclinedException(reason, detail));
            var charge = new ModelCharge(memory);
            long charged = 0;
            byte[]? raw = null;
            try
            {
                raw = memory.Allocate((int)footerLength);
                file.Position = dataEnd;
                file.ReadExactly(raw);
                DataFileSchema? schema = null;
                bool[]? read = null;
                var footer = ParquetFooterModel.Parse(raw, elements =>
                {
                    schema = DataFileSchema.Build(elements);
                    read = project(schema);
                    return read;
                }, charge, dataFile: true);
                memory.Free(raw);
                raw = null;
                charged = charge.Bytes;
                // A stored Arrow schema can change the reader's types
                if (footer.HasArrowSchema)
                {
                    throw new DataFileDeclinedException(CompactionReason.BindingPreflight, "The footer stores an Arrow schema");
                }
                var groups = ScanGroups(file, footer, schema!, read!, limits, memory, dataEnd, ref charged);
                return new DataFileScan(footer, schema!, Enumerable.Range(0, read!.Length).Where(x => read[x]).ToArray(), groups, charged);
            }
            catch (Exception e)
            {
                if (raw != null)
                {
                    memory.Free(raw);
                }
                memory.Release(Math.Max(charged, charge.Bytes));
                var declined = Decline(e);
                if (declined == null || declined == e)
                {
                    throw;
                }
                throw declined;
            }
        }

        // A scan failure's decline, null when it isn't the file's
        private static DataFileDeclinedException? Decline(Exception e) => e switch
        {
            DataFileDeclinedException declined => declined,
            ThriftFormatException thrift => new DataFileDeclinedException(thrift.Reason, thrift.Message),
            PageScanException page => new DataFileDeclinedException(page.Reason, page.Message),
            CheckpointDeclinedException { Reason: MaintenanceReason.ManagedMemoryCap } => new DataFileDeclinedException(CompactionReason.ManagedBound, e.Message),
            OverflowException => new DataFileDeclinedException(CompactionReason.BoundOverflow, e.Message),
            _ when IsMalformed(e) => new DataFileDeclinedException(CompactionReason.Malformed, e.Message),
            _ => null
        };

        private static LeafTerms[][] ScanGroups(FileStream file, ParquetFooterModel footer, DataFileSchema schema, bool[] read, ScanLimits limits, JobMemory memory, long dataEnd, ref long charged)
        {
            if (footer.NumRows < 0)
            {
                throw new PageScanException("The footer has no row count", CompactionReason.MissingField);
            }
            if (footer.NumRows != footer.RowGroupRows.Sum())
            {
                throw new PageScanException($"The footer has {footer.NumRows} rows and its row groups {footer.RowGroupRows.Sum()}");
            }
            var readLeaves = Enumerable.Range(0, read.Length).Where(x => read[x]).ToArray();
            if (readLeaves.Length == 0)
            {
                throw new PageScanException("No leaf of the file is read", CompactionReason.BindingPreflight);
            }
            var table = checked(TermsBytes * readLeaves.Length * footer.RowGroupRows.Count);
            memory.Reserve(table);
            charged += table;
            var groups = new LeafTerms[footer.RowGroupRows.Count][];
            var header = memory.Allocate(limits.HeaderMax);
            try
            {
                for (int g = 0; g < groups.Length; g++)
                {
                    var rows = footer.RowGroupRows[g];
                    var extents = new List<(long Start, long End)>();
                    groups[g] = new LeafTerms[readLeaves.Length];
                    for (int i = 0; i < readLeaves.Length; i++)
                    {
                        var node = schema.Leaves[readLeaves[i]];
                        var chunk = footer.Chunks[g][readLeaves[i]]!;
                        if (chunk.Codec is Lzo or Brotli || chunk.Codec < 0 || chunk.Codec > Lz4Raw)
                        {
                            throw new PageScanException($"Leaf {node.Path} uses codec {chunk.Codec}", CompactionReason.UnsupportedCodec);
                        }
                        if (chunk.PhysicalType != (int)node.Element.Type!.Value)
                        {
                            throw new PageScanException($"Leaf {node.Path} has type {chunk.PhysicalType} in its chunk and {node.Element.Type} in the schema");
                        }
                        // Arrow reads to the value count, and every row has a level
                        if (node.Repeated ? chunk.NumValues < rows : chunk.NumValues != rows)
                        {
                            throw new PageScanException($"Leaf {node.Path} holds {chunk.NumValues} values in a row group of {rows} rows");
                        }
                        var start = chunk.DictionaryPageOffset is > 0 and var dictionary && dictionary < chunk.DataPageOffset ? dictionary : chunk.DataPageOffset;
                        if (start < 4 || start > dataEnd || chunk.TotalCompressedSize > dataEnd - start)
                        {
                            throw new PageScanException($"Leaf {node.Path} has a chunk outside the data region");
                        }
                        extents.Add((start, start + chunk.TotalCompressedSize));
                        var leaf = new ScanLeaf(node.Path, node.Element.Type.Value, node.MaxDefinition, node.MaxRepetition, node.Element.TypeLength);
                        var terms = CheckpointPageScan.ScanChunk(file, leaf, chunk, start, limits, header);
                        var compressed = chunk.Codec != 0;
                        // Arrow reads an uncompressed page by its compressed size
                        var understated = compressed ? 0 : Math.Max(0, terms.DictionaryCompressed - terms.DictionaryPage);
                        groups[g][i] = new LeafTerms()
                        {
                            Stream = terms.Stream,
                            Page = compressed ? terms.LargestUncompressed : Math.Max(terms.LargestUncompressed, terms.LargestCompressed),
                            Buffer = compressed ? terms.LargestUncompressed : terms.LargestCompressed,
                            Compressed = terms.LargestCompressed,
                            Dictionary = terms.Dictionary + understated,
                            DictionaryPage = terms.DictionaryPage + understated,
                            Header = terms.LargestHeader,
                            PageValues = terms.PageValues,
                            Values = chunk.NumValues,
                            Uncompressed = compressed ? chunk.TotalUncompressedSize : Math.Max(chunk.TotalUncompressedSize, chunk.TotalCompressedSize),
                            // Size statistics bound dictionary chunks, U bounds plain ones soundly
                            Unencoded = terms.Dictionary > 0 ? chunk.UnencodedBytes : null,
                            HasDictionary = terms.Dictionary > 0,
                            IsCompressed = compressed
                        };
                    }
                    extents.Sort();
                    for (int i = 1; i < extents.Count; i++)
                    {
                        if (extents[i].Start < extents[i - 1].End)
                        {
                            throw new PageScanException("Read chunks overlap");
                        }
                    }
                }
            }
            finally
            {
                memory.Free(header);
            }
            return groups;
        }

        // Arithmetic or indexing failures on input the checks above missed
        private static bool IsMalformed(Exception e) => e is IndexOutOfRangeException or ArgumentOutOfRangeException or OverflowException or InvalidCastException;
    }
}
