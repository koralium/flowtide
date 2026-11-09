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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded
{
    /// <summary>
    /// The input envelope of a root.
    /// </summary>
    internal sealed record ScanLimits
    {
        public static readonly ScanLimits Default = new ScanLimits();

        public long FooterMax { get; init; } = 16L << 20;

        public int HeaderMax { get; init; } = 32 << 10;

        public long PageMax { get; init; } = 16L << 20;

        public long DecodeCap { get; init; } = 128L << 20;

        public long MetadataCap { get; init; } = 32L << 20;

        public long BufferSize { get; init; } = CheckpointPageScan.ReaderBufferSize;
    }

    // A page the scan refuses, with the decline a data file gets
    internal sealed class PageScanException : Exception
    {
        public PageScanException(string message, CompactionReason reason = CompactionReason.Malformed) : base(message)
        {
            Reason = reason;
        }

        public CompactionReason Reason { get; }
    }

    // A leaf as the page scan sees it
    internal readonly record struct ScanLeaf(string Path, ParquetPhysicalType Type, int MaxDefinition, int MaxRepetition, int TypeLength);

    /// <summary>
    /// An admitted root: its footer model and projection, and the native terms the reader reserves before opening it.
    /// </summary>
    internal sealed record ScanResult(ParquetFooterModel Footer, CheckpointProjection Projection, long DecodeBound, long MetadataBound, long PoolVisible, long ModelBytes);

    /// <summary>
    /// The managed scan that admits a root before any native open.
    /// </summary>
    internal static class CheckpointPageScan
    {
        public const long ReaderBufferSize = 64L << 10;

        // The pinned page reader's first header peek and its doubling limit
        public const int FirstHeaderPeek = 16 << 10;
        public const int MaxHeaderPeek = 16 << 20;

        private const int Uncompressed = 0, Snappy = 1, Gzip = 2, Lzo = 3, Brotli = 4, Lz4 = 5, Zstd = 6, Lz4Raw = 7;
        private const int Plain = 0, PlainDictionary = 2, Rle = 3, BitPacked = 4, RleDictionary = 8;
        internal const int DataPage = 0, DictionaryPage = 2, DataPageV2 = 3;

        private static CheckpointDeclinedException Out(string detail)
        {
            return new CheckpointDeclinedException(MaintenanceReason.OutOfEnvelope, DeclineDependency.Root, detail);
        }

        public static ScanResult Scan(string path, JobMemory memory, ScanLimits? limits = null, Func<CheckpointProjection, CheckpointProjection>? select = null)
        {
            limits ??= ScanLimits.Default;
            // The replayed stream never reaches the GH-48311 trigger only above the first header peek
            if (limits.BufferSize <= FirstHeaderPeek)
            {
                throw new InvalidOperationException($"The reader buffer must stay above {FirstHeaderPeek} bytes");
            }
            using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1, FileOptions.RandomAccess);
            var (footerLength, dataEnd) = ReadFrame(file, limits, (detail, _) => Out(detail));

            // The raw footer is held only while it is parsed; the model is charged as it is built
            ParquetFooterModel footer;
            CheckpointProjection? projection = null;
            var charge = new ModelCharge(memory);
            var raw = memory.Allocate((int)footerLength);
            try
            {
                file.Position = dataEnd;
                file.ReadExactly(raw);
                footer = ParquetFooterModel.Parse(raw, schema =>
                {
                    projection = CheckpointProjection.Of(schema);
                    // A selection reads fewer leaves, and may read none, as classification does without a sidecar column
                    if (select != null)
                    {
                        projection = select(projection);
                    }
                    return projection.Mask();
                }, charge);
            }
            catch (Exception e) when (e is ThriftFormatException || IsMalformed(e))
            {
                memory.Release(charge.Bytes);
                throw Out(e.Message);
            }
            catch
            {
                memory.Release(charge.Bytes);
                throw;
            }
            finally
            {
                memory.Free(raw);
            }
            var modelBytes = charge.Bytes;
            try
            {
                return ScanPages(file, footer, projection!, limits, memory, dataEnd, modelBytes, select != null);
            }
            catch (Exception e) when (e is PageScanException || IsMalformed(e))
            {
                memory.Release(modelBytes);
                throw Out(e.Message);
            }
            catch
            {
                memory.Release(modelBytes);
                throw;
            }
        }

        // The magics and the footer's length; fail gets the data-file decline too
        internal static (long FooterLength, long DataEnd) ReadFrame(FileStream file, ScanLimits limits, Func<string, CompactionReason, Exception> fail)
        {
            var length = file.Length;
            if (length < 12)
            {
                throw fail("The file is too short for Parquet", CompactionReason.Malformed);
            }
            Span<byte> tail = stackalloc byte[8];
            file.Position = length - 8;
            file.ReadExactly(tail);
            if (tail[4] == 'P' && tail[5] == 'A' && tail[6] == 'R' && tail[7] == 'E')
            {
                throw fail("The file is encrypted", CompactionReason.Encrypted);
            }
            if (tail[4] != 'P' || tail[5] != 'A' || tail[6] != 'R' || tail[7] != '1')
            {
                throw fail("The footer magic is missing", CompactionReason.Malformed);
            }
            Span<byte> head = stackalloc byte[4];
            file.Position = 0;
            file.ReadExactly(head);
            if (head[0] != 'P' || head[1] != 'A' || head[2] != 'R' || head[3] != '1')
            {
                throw fail("The leading magic is missing", CompactionReason.Malformed);
            }
            long footerLength = ThriftCompactReader.ReadInt32LittleEndian(tail);
            if (footerLength <= 0 || footerLength > length - 12)
            {
                throw fail($"A footer of {footerLength} bytes", CompactionReason.Malformed);
            }
            if (footerLength > limits.FooterMax)
            {
                throw fail($"A footer of {footerLength} bytes", CompactionReason.MetadataBound);
            }
            return (footerLength, length - 8 - footerLength);
        }

        private static int BitWidth(int maxLevel) => maxLevel == 0 ? 0 : 32 - System.Numerics.BitOperations.LeadingZeroCount((uint)maxLevel);

        private static void CheckV2Levels(FileStream file, long body, PageHeader page, ScanLeaf leaf, byte[] buffer)
        {
            long rows = page.Values;
            long nulls = 0;
            if (leaf.MaxRepetition > 0)
            {
                rows = CountLevels(file, body, page.RepetitionLength, BitWidth(leaf.MaxRepetition), page.Values, 0, leaf.MaxRepetition, buffer, leaf);
            }
            else if (page.RepetitionLength != 0)
            {
                throw new PageScanException($"Leaf {leaf.Path}: repetition levels on a leaf without repetition");
            }
            if (leaf.MaxDefinition > 0)
            {
                // Levels below the maximum are the nulls; counting the maximum gives the defined values
                var defined = CountLevels(file, body + page.RepetitionLength, page.DefinitionLength, BitWidth(leaf.MaxDefinition), page.Values, leaf.MaxDefinition, leaf.MaxDefinition, buffer, leaf);
                nulls = page.Values - defined;
            }
            else if (page.DefinitionLength != 0)
            {
                throw new PageScanException($"Leaf {leaf.Path}: definition levels on a required leaf");
            }
            if (rows != page.Rows || nulls != page.Nulls)
            {
                throw new PageScanException($"Leaf {leaf.Path}: a V2 page declares {page.Rows} rows and {page.Nulls} nulls, its levels hold {rows} and {nulls}");
            }
        }

        // Decodes exactly count levels of the RLE and bit-packed hybrid and counts those equal to the value; an early end is refused
        private static long CountLevels(FileStream file, long offset, int length, int bitWidth, int count, int value, int max, byte[] buffer, ScanLeaf leaf)
        {
            var source = new RegionReader(file, offset, length, buffer);
            long matched = 0;
            long decoded = 0;
            var valueBytes = (bitWidth + 7) / 8;
            while (decoded < count)
            {
                long header = 0;
                for (int shift = 0; ; shift += 7)
                {
                    if (shift > 28)
                    {
                        throw new PageScanException($"Leaf {leaf.Path}: a level run header is too long");
                    }
                    var b = source.Next(leaf);
                    header |= (long)(b & 0x7F) << shift;
                    if ((b & 0x80) == 0)
                    {
                        break;
                    }
                }
                if ((header & 1) == 0)
                {
                    var run = header >> 1;
                    var level = 0;
                    for (int i = 0; i < valueBytes; i++)
                    {
                        level |= source.Next(leaf) << (8 * i);
                    }
                    var taken = Math.Min(run, count - decoded);
                    CheckLevel(level, max, leaf);
                    if (level == value)
                    {
                        matched += taken;
                    }
                    decoded += taken;
                    if (run == 0)
                    {
                        throw new PageScanException($"Leaf {leaf.Path}: an empty level run");
                    }
                }
                else
                {
                    // Groups of eight levels, least significant bits first; the padding after the last level is not read, as Arrow does not
                    var groups = header >> 1;
                    if (groups == 0)
                    {
                        throw new PageScanException($"Leaf {leaf.Path}: an empty bit-packed run");
                    }
                    long bits = 0;
                    var held = 0;
                    for (long i = 0; i < groups * 8 && decoded < count; i++)
                    {
                        while (held < bitWidth)
                        {
                            bits |= (long)source.Next(leaf) << held;
                            held += 8;
                        }
                        var level = (int)(bits & ((1L << bitWidth) - 1));
                        bits >>= bitWidth;
                        held -= bitWidth;
                        CheckLevel(level, max, leaf);
                        matched += level == value ? 1 : 0;
                        decoded++;
                    }
                }
            }
            return matched;
        }

        // Arrow refuses a level above the leaf's maximum
        private static void CheckLevel(int level, int max, ScanLeaf leaf)
        {
            if (level > max)
            {
                throw new PageScanException($"Leaf {leaf.Path}: a level of {level} above its maximum {max}");
            }
        }

        // The bytes of a region of the file, read through the scan's header buffer
        private sealed class RegionReader
        {
            private readonly FileStream _file;
            private readonly byte[] _buffer;
            private long _position;
            private long _remaining;
            private int _start;
            private int _end;

            public RegionReader(FileStream file, long offset, int length, byte[] buffer)
            {
                _file = file;
                _buffer = buffer;
                _position = offset;
                _remaining = length;
            }

            public int Next(ScanLeaf leaf)
            {
                if (_start == _end)
                {
                    if (_remaining == 0)
                    {
                        throw new PageScanException($"Leaf {leaf.Path}: a V2 level section ends before its levels");
                    }
                    var count = (int)Math.Min(_buffer.Length, _remaining);
                    _file.Position = _position;
                    _file.ReadExactly(_buffer, 0, count);
                    _position += count;
                    _remaining -= count;
                    _start = 0;
                    _end = count;
                }
                return _buffer[_start++];
            }
        }

        /// <summary>
        /// Whether a file has no valid Parquet frame: too short, a missing magic or a footer length that does not fit.
        /// </summary>
        public static bool IsTorn(string path)
        {
            using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, 1, FileOptions.RandomAccess);
            var length = file.Length;
            if (length < 12)
            {
                return true;
            }
            Span<byte> tail = stackalloc byte[8];
            file.Position = length - 8;
            file.ReadExactly(tail);
            Span<byte> head = stackalloc byte[4];
            file.Position = 0;
            file.ReadExactly(head);
            var footerLength = ThriftCompactReader.ReadInt32LittleEndian(tail);
            var magic = tail[4] == 'P' && tail[5] == 'A' && tail[6] == 'R' && (tail[7] == '1' || tail[7] == 'E');
            return !magic || head[0] != 'P' || head[1] != 'A' || head[2] != 'R' || head[3] != (tail[7] == 'E' ? 'E' : '1')
                || footerLength <= 0 || footerLength > length - 12;
        }

        // Arithmetic or indexing failures on input the checks above missed; I/O errors stay transient
        private static bool IsMalformed(Exception e) => e is IndexOutOfRangeException or ArgumentOutOfRangeException or OverflowException or InvalidCastException;

        private static bool TypeAllowed(ProjectedLeaf leaf)
        {
            return leaf.Role switch
            {
                ProjectedRole.Field => CheckpointSchema.Type(leaf.Field) switch
                {
                    LeafType.String => leaf.Type == ParquetPhysicalType.ByteArray,
                    LeafType.Int64 => leaf.Type == ParquetPhysicalType.Int64,
                    LeafType.Int32 => leaf.Type == ParquetPhysicalType.Int32,
                    _ => leaf.Type == ParquetPhysicalType.Boolean
                },
                ProjectedRole.SidecarPath => leaf.Type == ParquetPhysicalType.ByteArray,
                ProjectedRole.StatsNumRecords => leaf.Type == ParquetPhysicalType.Int64,
                ProjectedRole.StatsTightBounds => leaf.Type == ParquetPhysicalType.Boolean,
                _ => leaf.Type is ParquetPhysicalType.ByteArray or ParquetPhysicalType.Int64 or ParquetPhysicalType.Int32 or ParquetPhysicalType.Boolean
            };
        }

        private static bool ValueEncodingAllowed(ParquetPhysicalType type, int encoding)
        {
            return type == ParquetPhysicalType.Boolean ? encoding is Plain or Rle : encoding is Plain or PlainDictionary or RleDictionary;
        }

        internal sealed class ChunkTerms
        {
            public long Stream;
            public long LargestUncompressed;
            public long LargestCompressed;
            public long Dictionary;
            // The dictionary page itself, no value is larger
            public long DictionaryPage;
            public long DictionaryCompressed;
            public long LargestHeader;
            // The most values one data page holds
            public long PageValues;
        }

        private static ScanResult ScanPages(FileStream file, ParquetFooterModel footer, CheckpointProjection projection, ScanLimits limits, JobMemory memory, long dataEnd, long modelBytes, bool selected)
        {
            if (projection.Leaves.Count == 0 && !selected)
            {
                throw Out("No leaf of the file is read");
            }
            foreach (var leaf in projection.Leaves)
            {
                if (!TypeAllowed(leaf))
                {
                    throw Out($"Leaf {leaf.LogicalPath} has physical type {leaf.Type}");
                }
            }
            var header = memory.Allocate(limits.HeaderMax);
            try
            {
                long decode = 0;
                long poolVisible = 0;
                for (int g = 0; g < footer.RowGroupRows.Count; g++)
                {
                    var chunks = footer.Chunks[g];
                    var extents = new List<(long Start, long End)>();
                    long groupTerms = 0;
                    long groupPool = 0;
                    long largestBody = 0;
                    long overlap = 0;
                    foreach (var leaf in projection.Leaves)
                    {
                        var chunk = chunks[leaf.Index]!;
                        if (chunk.Codec is Lzo or Brotli || chunk.Codec < Uncompressed || chunk.Codec > Lz4Raw)
                        {
                            throw Out($"Leaf {leaf.LogicalPath} uses codec {chunk.Codec}");
                        }
                        var start = chunk.DictionaryPageOffset is > 0 and var dictionary && dictionary < chunk.DataPageOffset ? dictionary : chunk.DataPageOffset;
                        if (start < 4 || start > dataEnd || chunk.TotalCompressedSize < 0 || chunk.TotalCompressedSize > dataEnd - start)
                        {
                            throw Out($"Leaf {leaf.LogicalPath} has a chunk outside the data region");
                        }
                        extents.Add((start, start + chunk.TotalCompressedSize));
                        var terms = ScanChunk(file, new ScanLeaf(leaf.LogicalPath, leaf.Type, leaf.MaxDefinition, leaf.MaxRepetition, 0), chunk, start, limits, header);
                        var compressed = chunk.Codec != Uncompressed;
                        // An uncompressed body is the page itself and counted once
                        var uncompressedBuffer = compressed ? terms.LargestUncompressed : terms.LargestCompressed;
                        groupTerms += terms.Stream + uncompressedBuffer + terms.Dictionary + 2 * terms.LargestHeader + NativeConstants.CodecPerLeaf;
                        // The Advance copy of a header comes from the pool, the heap PageHeader does not
                        groupPool += terms.Stream + uncompressedBuffer + terms.Dictionary + terms.LargestHeader;
                        if (compressed)
                        {
                            largestBody = Math.Max(largestBody, terms.LargestCompressed);
                        }
                        overlap = Math.Max(overlap, Math.Max(terms.Stream, Math.Max(uncompressedBuffer, terms.Dictionary)));
                    }
                    extents.Sort();
                    for (int i = 1; i < extents.Count; i++)
                    {
                        if (extents[i].Start < extents[i - 1].End)
                        {
                            throw Out("Projected chunks overlap");
                        }
                    }
                    decode = Math.Max(decode, groupTerms + largestBody + overlap);
                    poolVisible = Math.Max(poolVisible, groupPool + largestBody + NativeConstants.PoolPerLeaf * projection.Leaves.Count);
                }
                decode += NativeConstants.ReaderFixed;
                // The footer is read into a pool buffer before any page, at least the speculative read
                poolVisible = Math.Max(poolVisible, Math.Max(footer.FooterLength + 8, Math.Min(NativeConstants.FooterReadSize, dataEnd + footer.FooterLength + 8)));
                var metadata = footer.FooterLength
                    + NativeConstants.ChunkMetadata * footer.TotalChunks
                    + NativeConstants.SchemaElementMetadata * footer.Schema.Count
                    + NativeConstants.RowGroupMetadata * footer.RowGroupRows.Count
                    + NativeConstants.StringFactor * footer.StringBytes
                    + NativeConstants.ListEntryBytes * footer.ListEntries;
                if (decode > limits.DecodeCap)
                {
                    throw Out($"A decode bound of {decode} bytes over the {limits.DecodeCap} byte cap");
                }
                if (metadata > limits.MetadataCap)
                {
                    throw Out($"A metadata bound of {metadata} bytes over the {limits.MetadataCap} byte cap");
                }
                return new ScanResult(footer, projection, decode, metadata, poolVisible, modelBytes);
            }
            finally
            {
                memory.Free(header);
            }
        }

        // Every page header of one chunk, checked against the footer
        internal static ChunkTerms ScanChunk(FileStream file, ScanLeaf leaf, FooterChunk chunk, long start, ScanLimits limits, byte[] header)
        {
            var terms = new ChunkTerms();
            var stream = new StreamReplay(chunk.TotalCompressedSize, limits.BufferSize);
            long consumed = 0, uncompressedTotal = 0, values = 0;
            var pages = 0;
            var dictionaryPages = 0;
            foreach (var encoding in chunk.Encodings)
            {
                if (encoding is not (Plain or PlainDictionary or Rle or BitPacked or RleDictionary))
                {
                    throw new PageScanException($"Leaf {leaf.Path} lists encoding {encoding}", CompactionReason.UnsupportedEncoding);
                }
            }
            while (consumed < chunk.TotalCompressedSize)
            {
                var available = (int)Math.Min(header.Length, chunk.TotalCompressedSize - consumed);
                file.Position = start + consumed;
                file.ReadExactly(header, 0, available);
                PageHeader page;
                int headerLength;
                try
                {
                    var reader = new ThriftCompactReader(header.AsSpan(0, available), new ThriftBudget(limits.HeaderMax));
                    page = PageHeader.Read(ref reader);
                    headerLength = reader.Position;
                }
                catch (ThriftFormatException e)
                {
                    throw new PageScanException($"Leaf {leaf.Path}: a page header is malformed or over {limits.HeaderMax} bytes ({e.Message})", e.Reason);
                }
                if (page.Compressed < 0 || page.Uncompressed < 0 || page.Compressed > limits.PageMax || page.Uncompressed > limits.PageMax)
                {
                    throw new PageScanException($"Leaf {leaf.Path}: a page of {page.Compressed} compressed and {page.Uncompressed} uncompressed bytes");
                }
                if (consumed + headerLength + page.Compressed > chunk.TotalCompressedSize)
                {
                    throw new PageScanException($"Leaf {leaf.Path}: page sizes disagree with the footer");
                }
                stream.Page(headerLength, page.Compressed);
                switch (page.Type)
                {
                    case DictionaryPage:
                        if (pages > 0 || dictionaryPages > 0 || leaf.Type == ParquetPhysicalType.Boolean)
                        {
                            throw new PageScanException($"Leaf {leaf.Path}: a dictionary page out of place");
                        }
                        if (page.Encoding is not (Plain or PlainDictionary))
                        {
                            throw new PageScanException($"Leaf {leaf.Path}: a dictionary page with encoding {page.Encoding}", CompactionReason.UnsupportedEncoding);
                        }
                        // 4 B per byte-array length; checkpoints don't read FLBA lengths
                        var width = leaf.Type switch
                        {
                            ParquetPhysicalType.Int64 or ParquetPhysicalType.Double => 8L,
                            ParquetPhysicalType.Int96 => 12L,
                            ParquetPhysicalType.FixedLenByteArray when leaf.TypeLength > 0 => leaf.TypeLength,
                            _ => 4L
                        };
                        if (page.Values < 0 || page.Values * width > page.Uncompressed)
                        {
                            throw new PageScanException($"Leaf {leaf.Path}: a dictionary of {page.Values} entries in {page.Uncompressed} bytes");
                        }
                        // Arrow keeps 16 B per byte-array entry, 8 B per FLBA entry
                        terms.Dictionary = leaf.Type switch
                        {
                            ParquetPhysicalType.ByteArray => 16 * page.Values + page.Uncompressed + 4 * (page.Values + 1),
                            ParquetPhysicalType.FixedLenByteArray => page.Values * (width + 8),
                            _ => page.Values * width
                        } + 4 * 1024;
                        terms.DictionaryPage = page.Uncompressed;
                        terms.DictionaryCompressed = page.Compressed;
                        dictionaryPages++;
                        break;
                    case DataPage:
                        if (!ValueEncodingAllowed(leaf.Type, page.Encoding) || page.DefinitionEncoding is not (Rle or BitPacked) || page.RepetitionEncoding is not (Rle or BitPacked))
                        {
                            throw new PageScanException($"Leaf {leaf.Path}: a data page with encodings {page.Encoding}, {page.DefinitionEncoding}, {page.RepetitionEncoding}", CompactionReason.UnsupportedEncoding);
                        }
                        values += page.Values;
                        terms.PageValues = Math.Max(terms.PageValues, page.Values);
                        break;
                    case DataPageV2:
                        if (!ValueEncodingAllowed(leaf.Type, page.Encoding))
                        {
                            throw new PageScanException($"Leaf {leaf.Path}: a V2 page with encoding {page.Encoding}", CompactionReason.UnsupportedEncoding);
                        }
                        if (page.Nulls < 0 || page.Rows < 0 || page.Nulls > page.Values || page.Rows > page.Values
                            || page.DefinitionLength < 0 || page.RepetitionLength < 0 || (long)page.DefinitionLength + page.RepetitionLength > Math.Min(page.Compressed, page.Uncompressed)
                            || (!page.IsCompressed && page.Compressed != page.Uncompressed))
                        {
                            throw new PageScanException($"Leaf {leaf.Path}: an invalid V2 page header");
                        }
                        values += page.Values;
                        terms.PageValues = Math.Max(terms.PageValues, page.Values);
                        // A V2 page's levels are stored uncompressed, so its row and null counts are checked here, page by page
                        CheckV2Levels(file, start + consumed + headerLength, page, leaf, header);
                        break;
                    default:
                        throw new PageScanException($"Leaf {leaf.Path}: page type {page.Type}");
                }
                if (page.Values < 0)
                {
                    throw new PageScanException($"Leaf {leaf.Path}: a page with {page.Values} values");
                }
                pages++;
                consumed += headerLength + page.Compressed;
                uncompressedTotal += headerLength + page.Uncompressed;
                terms.LargestHeader = Math.Max(terms.LargestHeader, headerLength);
                terms.LargestCompressed = Math.Max(terms.LargestCompressed, page.Compressed);
                terms.LargestUncompressed = Math.Max(terms.LargestUncompressed, page.Uncompressed);
            }
            if (consumed != chunk.TotalCompressedSize || uncompressedTotal != chunk.TotalUncompressedSize || values != chunk.NumValues)
            {
                throw new PageScanException($"Leaf {leaf.Path}: page sizes or value counts disagree with the footer");
            }

            terms.Stream = stream.Peak;
            return terms;
        }
    }

    /// <summary>
    /// A replay of the pinned BufferedInputStream's growth, refusing the GH-48311 pattern.
    /// </summary>
    internal sealed class StreamReplay
    {
        private readonly long _bound;
        private long _bufferSize;
        private long _position;
        private long _buffered;
        private long _rawTotal;

        public StreamReplay(long chunkLength, long bufferSize)
        {
            _bound = chunkLength;
            // Create sets the size through SetBufferSize with nothing buffered
            _bufferSize = Math.Min(bufferSize, chunkLength);
            Peak = _bufferSize;
        }

        public long Peak { get; private set; }

        public void Page(long header, long body)
        {
            long allowed = CheckpointPageScan.FirstHeaderPeek;
            while (true)
            {
                var view = Peek(allowed);
                if (view >= header)
                {
                    break;
                }
                allowed *= 2;
                if (view == 0 || allowed > CheckpointPageScan.MaxHeaderPeek)
                {
                    throw new PageScanException("The page reader could not read a header");
                }
            }
            Read(header);
            Read(body);
        }

        private long Peek(long requested)
        {
            var nbytes = Math.Min(requested, _buffered + (_bound - _rawTotal));
            if (_buffered == 0 && nbytes < _bufferSize)
            {
                DoBuffer();
            }
            if (nbytes > _bufferSize - _position)
            {
                SetBufferSize(nbytes + _position);
            }
            if (nbytes > _buffered)
            {
                var additional = Math.Min(nbytes - _buffered, _bound - _rawTotal);
                // GH-48311: the read would run past the buffer's end
                if (_position + _buffered + additional > _bufferSize)
                {
                    throw new PageScanException("The page reader would overflow its stream buffer (GH-48311)");
                }
                _buffered += additional;
                _rawTotal += additional;
                nbytes = _buffered;
            }
            return nbytes;
        }

        private void SetBufferSize(long size)
        {
            if (_position + _buffered >= size)
            {
                throw new PageScanException("The page reader's stream buffer cannot shrink");
            }
            size = _buffered == 0 ? Math.Min(size, _bound - _rawTotal) : Math.Min(size, _position + _buffered + (_bound - _rawTotal));
            _bufferSize = size;
            Peak = Math.Max(Peak, size);
        }

        private void DoBuffer()
        {
            var bytes = Math.Min(_bufferSize, _bound - _rawTotal);
            _buffered = bytes;
            _position = 0;
            _rawTotal += bytes;
        }

        private void Read(long nbytes)
        {
            var pre = Math.Min(nbytes, _buffered);
            _position += pre;
            _buffered -= pre;
            var remaining = Math.Min(nbytes - pre, _bound - _rawTotal);
            if (remaining == 0)
            {
                return;
            }
            if (remaining >= _bufferSize)
            {
                _rawTotal += remaining;
                _position = 0;
                _buffered = 0;
                return;
            }
            DoBuffer();
            var copy = Math.Min(_buffered, remaining);
            _position += copy;
            _buffered -= copy;
        }
    }

    /// <summary>
    /// The fields of a Parquet PageHeader the scan checks, taken from the one sub-header its page type names.
    /// </summary>
    internal struct PageHeader
    {
        public int Type;
        public int Uncompressed;
        public int Compressed;
        public int Values;
        public int Encoding;
        public int DefinitionEncoding;
        public int RepetitionEncoding;
        public int Nulls;
        public int Rows;
        public int DefinitionLength;
        public int RepetitionLength;
        public bool IsCompressed;

        public static PageHeader Read(ref ThriftCompactReader reader)
        {
            var header = new PageHeader() { Type = -1, Uncompressed = -1, Compressed = -1 };
            SubHeader? data = null, dictionary = null, v2 = null;
            reader.EnterStruct();
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var type))
            {
                switch (id)
                {
                    case 1:
                        header.Type = reader.ReadI32(type);
                        break;
                    case 2:
                        header.Uncompressed = reader.ReadI32(type);
                        break;
                    case 3:
                        header.Compressed = reader.ReadI32(type);
                        break;
                    case 5:
                        data = SubHeader.Read(ref reader, type, id);
                        break;
                    case 7:
                        dictionary = SubHeader.Read(ref reader, type, id);
                        break;
                    case 8:
                        v2 = SubHeader.Read(ref reader, type, id);
                        break;
                    default:
                        reader.Skip(type);
                        break;
                }
            }
            reader.LeaveStruct();
            // Only the sub-header of the page's own type counts, so any other one is refused rather than ignored
            var (selected, others) = header.Type switch
            {
                CheckpointPageScan.DataPage => (data, dictionary is not null || v2 is not null),
                CheckpointPageScan.DictionaryPage => (dictionary, data is not null || v2 is not null),
                CheckpointPageScan.DataPageV2 => (v2, data is not null || dictionary is not null),
                _ => (null, false)
            };
            if (header.Type is CheckpointPageScan.DataPage or CheckpointPageScan.DictionaryPage or CheckpointPageScan.DataPageV2)
            {
                if (selected is not { } sub || others)
                {
                    throw new ThriftFormatException($"A page of type {header.Type} without its own sub-header, or with another one");
                }
                header.Values = sub.Values;
                header.Encoding = sub.Encoding;
                header.DefinitionEncoding = sub.DefinitionEncoding;
                header.RepetitionEncoding = sub.RepetitionEncoding;
                header.Nulls = sub.Nulls;
                header.Rows = sub.Rows;
                header.DefinitionLength = sub.DefinitionLength;
                header.RepetitionLength = sub.RepetitionLength;
                header.IsCompressed = sub.IsCompressed;
            }
            return header;
        }

        private struct SubHeader
        {
            public int Values;
            public int Encoding;
            public int DefinitionEncoding;
            public int RepetitionEncoding;
            public int Nulls;
            public int Rows;
            public int DefinitionLength;
            public int RepetitionLength;
            public bool IsCompressed;

            public static SubHeader Read(ref ThriftCompactReader reader, ThriftType type, short which)
            {
                if (type != ThriftType.Struct)
                {
                    throw new ThriftFormatException("A page sub-header is not a struct");
                }
                var header = new SubHeader() { Values = -1, Encoding = -1, DefinitionEncoding = 3, RepetitionEncoding = 3, IsCompressed = true };
                reader.EnterStruct();
                short last = 0;
                ulong seen = 0;
                while (reader.ReadField(ref last, ref seen, out var id, out var fieldType))
                {
                    switch ((which, id))
                    {
                        case (5, 1):
                        case (7, 1):
                        case (8, 1):
                            header.Values = reader.ReadI32(fieldType);
                            break;
                        case (5, 2):
                        case (7, 2):
                        case (8, 4):
                            header.Encoding = reader.ReadI32(fieldType);
                            break;
                        case (5, 3):
                            header.DefinitionEncoding = reader.ReadI32(fieldType);
                            break;
                        case (5, 4):
                            header.RepetitionEncoding = reader.ReadI32(fieldType);
                            break;
                        case (8, 2):
                            header.Nulls = reader.ReadI32(fieldType);
                            break;
                        case (8, 3):
                            header.Rows = reader.ReadI32(fieldType);
                            break;
                        case (8, 5):
                            header.DefinitionLength = reader.ReadI32(fieldType);
                            break;
                        case (8, 6):
                            header.RepetitionLength = reader.ReadI32(fieldType);
                            break;
                        case (8, 7):
                            header.IsCompressed = ThriftCompactReader.ReadBool(fieldType);
                            break;
                        default:
                            reader.Skip(fieldType);
                            break;
                    }
                }
                reader.LeaveStruct();
                // Arrow's required fields: V1 1-4, dictionary 1-2, V2 1-6
                var required = which switch { 5 => 0b1_1110UL, 7 => 0b110UL, _ => 0b111_1110UL };
                if ((seen & required) != required)
                {
                    throw new ThriftFormatException($"A page sub-header {which} misses a required field", CompactionReason.MissingField);
                }
                return header;
            }
        }
    }
}
