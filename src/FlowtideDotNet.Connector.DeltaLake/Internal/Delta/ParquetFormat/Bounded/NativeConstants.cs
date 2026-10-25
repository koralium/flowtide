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

using ParquetSharp;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded
{
    internal enum AuditStatus
    {
        // A complete upper-bound derivation over admitted inputs, with the construction path in the pinned sources
        Resolved,
        // No complete derivation; usable only when accepted at the gate
        Unresolved
    }

    /// <summary>
    /// One row of native-audit.md: the reserved value, its derived bound when resolved, and the largest fresh-process fit.
    /// </summary>
    internal sealed record NativeTerm(string Id, long Reserved, long? DerivedBound, long? Fit, AuditStatus Status, bool AcceptedAtGate);

    /// <summary>
    /// Native allocation terms of the pinned ParquetSharp, one audit row each in native-audit.md.
    /// </summary>
    internal static class NativeConstants
    {
        public const string PinnedParquetSharp = "23.0.0.2";

        // Reader footer metadata
        public const long ChunkMetadata = 1536;
        public const long SchemaElementMetadata = 1792;
        public const long RowGroupMetadata = 1024;
        public const long StringFactor = 2;
        public const long ListEntryBytes = 4;

        // Reader decode
        public const long CodecPerLeaf = 1L << 20;
        public const long ReaderFixed = 16L << 20;
        public const long HeaderBound = 2L << 10;

        // Pool allocations of each column reader outside the page terms
        public const long PoolPerLeaf = 1L << 10;

        // The reader's speculative footer read
        public const long FooterReadSize = 64L << 10;

        // Writer memory
        public const long WriterChunkMetadata = 1536;
        public const long WriterCodec = 3L << 20;
        public const long WriterFixed = 8L << 20;

        // Footer bytes our own output spends per column chunk, for its projected metadata
        public const long OwnFooterBytesPerChunk = 160;
        public const long OwnFooterStringBytesPerChunk = 40;
        public const long OwnFooterListEntriesPerChunk = 6;
        public const long OwnFooterFixed = 64L << 10;

        /// <summary>
        /// Every constant above, with its audit status and the fit measured on 2026-10-11.
        /// </summary>
        public static readonly IReadOnlyList<NativeTerm> Audit = new[]
        {
            new NativeTerm(nameof(ChunkMetadata), ChunkMetadata, null, 950, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(SchemaElementMetadata), SchemaElementMetadata, null, 1100, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(RowGroupMetadata), RowGroupMetadata, null, 331, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(StringFactor), StringFactor, null, 1, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(ListEntryBytes), ListEntryBytes, null, null, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(CodecPerLeaf), CodecPerLeaf, null, null, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(ReaderFixed), ReaderFixed, null, 1_031_000, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(HeaderBound), HeaderBound, null, 100, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(PoolPerLeaf), PoolPerLeaf, null, 50, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(FooterReadSize), FooterReadSize, null, null, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(WriterChunkMetadata), WriterChunkMetadata, null, null, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(WriterCodec), WriterCodec, null, null, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(WriterFixed), WriterFixed, null, null, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(OwnFooterBytesPerChunk), OwnFooterBytesPerChunk, null, null, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(OwnFooterStringBytesPerChunk), OwnFooterStringBytesPerChunk, null, null, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(OwnFooterListEntriesPerChunk), OwnFooterListEntriesPerChunk, null, null, AuditStatus.Unresolved, true),
            new NativeTerm(nameof(OwnFooterFixed), OwnFooterFixed, null, null, AuditStatus.Unresolved, true)
        };

        /// <summary>
        /// The codec's worst-case compressed size of n bytes, and whether it needs a compression scratch buffer.
        /// </summary>
        public static long Compressed(Compression compression, long bytes) => compression switch
        {
            Compression.Uncompressed => bytes,
            Compression.Snappy => bytes + bytes / 6 + 32,
            // zlib's conservative deflateBound, which applies when the level reaches zlib as another parameter; not checked in the pinned source
            Compression.Gzip => bytes + (bytes + 7) / 8 + (bytes + 63) / 64 + 1024,
            _ => bytes + bytes / 100 + 1024
        };

        public static bool HasScratch(Compression compression) => compression != Compression.Uncompressed;
    }
}
