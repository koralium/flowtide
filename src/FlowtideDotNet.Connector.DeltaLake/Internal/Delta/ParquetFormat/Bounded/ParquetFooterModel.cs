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
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded
{
    internal enum ParquetPhysicalType
    {
        Boolean = 0,
        Int32 = 1,
        Int64 = 2,
        Int96 = 3,
        Float = 4,
        Double = 5,
        ByteArray = 6,
        FixedLenByteArray = 7
    }

    internal enum ParquetRepetition
    {
        Required = 0,
        Optional = 1,
        Repeated = 2
    }

    internal sealed class FooterSchemaElement
    {
        public ParquetPhysicalType? Type { get; init; }

        public ParquetRepetition Repetition { get; init; }

        public required string Name { get; init; }

        public int Children { get; init; }

        // ConvertedType MAP, MAP_KEY_VALUE or LIST, or the matching logical type
        public bool IsMapOrList { get; init; }
    }

    /// <summary>
    /// The fields of one column chunk the scan needs; kept only for projected leaves.
    /// </summary>
    internal sealed class FooterChunk
    {
        public int Codec { get; init; }

        public required int[] Encodings { get; init; }

        public long NumValues { get; init; }

        public long TotalUncompressedSize { get; init; }

        public long TotalCompressedSize { get; init; }

        public long DataPageOffset { get; init; }

        public long? DictionaryPageOffset { get; init; }

        // Whether the chunk names an offset or column index, which our own output never writes
        public bool HasPageIndex { get; set; }
    }

    /// <summary>
    /// The managed bytes of a footer model, reserved in the job's ledger before each part is built.
    /// </summary>
    internal sealed class ModelCharge
    {
        private readonly JobMemory? _memory;

        public ModelCharge(JobMemory? memory)
        {
            _memory = memory;
        }

        public long Bytes { get; private set; }

        public void Add(long bytes)
        {
            _memory?.Reserve(bytes);
            Bytes += bytes;
        }
    }

    /// <summary>
    /// The managed model of a footer.
    /// </summary>
    internal sealed class ParquetFooterModel
    {
        public const int MaxRowGroups = 1024;
        public const int MaxSchemaElements = 4096;
        public const string MarkerKey = "flowtide.checkpoint";

        // Longer values are not kept: a marker past this many UTF-8 bytes is unmarked anyway, created_by is informational
        public const int MaxMarkerBytes = 4 * CoverageMarker.MaxLength;
        public const int MaxCreatedByBytes = 256;

        // The managed charge per object: a model element, a projected chunk, and what the projection keeps per schema element
        private const long ElementBytes = 96;
        private const long ChunkBytes = 160;
        private const long ProjectionBytesPerElement = 2048;

        public required List<FooterSchemaElement> Schema { get; init; }

        public required List<long> RowGroupRows { get; init; }

        // Per row group, the chunks of the projected leaves by leaf index; null where a leaf is not projected
        public required List<FooterChunk?[]> Chunks { get; init; }

        public string? CreatedBy { get; init; }

        public string? Marker { get; init; }

        public long FooterLength { get; init; }

        public long TotalChunks { get; init; }

        public long StringBytes { get; init; }

        public long ListEntries { get; init; }

        public long KeyValueBytes { get; init; }

        /// <summary>
        /// Parses a footer within its limits; once the schema is read, project tells which leaf indices (in file order) keep their chunk fields.
        /// </summary>
        public static ParquetFooterModel Parse(ReadOnlySpan<byte> footer, Func<IReadOnlyList<FooterSchemaElement>, bool[]> project, ModelCharge? charge = null)
        {
            charge ??= new ModelCharge(null);
            charge.Add(4096);
            bool[]? projection = null;
            var budget = new ThriftBudget(footer.Length);
            var reader = new ThriftCompactReader(footer, budget);
            var schema = new List<FooterSchemaElement>();
            var rowGroupRows = new List<long>();
            var chunks = new List<FooterChunk?[]>();
            string? createdBy = null;
            string? marker = null;
            long totalChunks = 0;
            long keyValueBytes = 0;
            var seen = 0;

            reader.EnterStruct();
            short last = 0;
            ulong fields = 0;
            while (reader.ReadField(ref last, ref fields, out var id, out var type))
            {
                switch (id)
                {
                    case 2:
                        {
                            var count = reader.ReadListHeader(type, out var element);
                            if (count > MaxSchemaElements)
                            {
                                throw new ThriftFormatException($"{count} schema elements");
                            }
                            for (int i = 0; i < count; i++)
                            {
                                schema.Add(ReadSchemaElement(ref reader, element, charge));
                            }
                            seen |= 1;
                            break;
                        }
                    case 4:
                        {
                            if ((seen & 1) == 0)
                            {
                                throw new ThriftFormatException("Row groups come before the schema");
                            }
                            if (projection == null)
                            {
                                // The projection walks every element and keeps its leaves
                                charge.Add(schema.Count * ProjectionBytesPerElement);
                                projection = project(schema);
                            }
                            var keep = projection;
                            var count = reader.ReadListHeader(type, out var element);
                            if (count > MaxRowGroups)
                            {
                                throw new ThriftFormatException($"{count} row groups");
                            }
                            for (int i = 0; i < count; i++)
                            {
                                ExpectStruct(element);
                                rowGroupRows.Add(ReadRowGroup(ref reader, keep, out var rowGroupChunks, ref totalChunks, charge));
                                chunks.Add(rowGroupChunks);
                            }
                            seen |= 2;
                            break;
                        }
                    case 5:
                        {
                            var count = reader.ReadListHeader(type, out var element);
                            for (int i = 0; i < count; i++)
                            {
                                ExpectStruct(element);
                                var (key, value, bytes) = ReadKeyValue(ref reader, charge);
                                keyValueBytes += bytes;
                                if (key == MarkerKey)
                                {
                                    marker = value;
                                }
                            }
                            break;
                        }
                    case 6:
                        var raw = reader.ReadBinary(type);
                        if (raw.Length <= MaxCreatedByBytes)
                        {
                            charge.Add(2L * raw.Length);
                            createdBy = Encoding.UTF8.GetString(raw);
                        }
                        break;
                    case 8:
                        throw new ThriftFormatException("The file is encrypted");
                    default:
                        reader.Skip(type);
                        break;
                }
            }
            reader.LeaveStruct();
            if (seen != 3)
            {
                throw new ThriftFormatException("The footer has no schema or row groups");
            }
            return new ParquetFooterModel()
            {
                Schema = schema,
                RowGroupRows = rowGroupRows,
                Chunks = chunks,
                CreatedBy = createdBy,
                Marker = marker,
                FooterLength = footer.Length,
                TotalChunks = totalChunks,
                StringBytes = budget.StringBytes,
                ListEntries = budget.ListEntries,
                KeyValueBytes = keyValueBytes
            };
        }

        private static void ExpectStruct(ThriftType element)
        {
            if (element != ThriftType.Struct)
            {
                throw new ThriftFormatException($"Expected a list of structs, found {element}");
            }
        }

        private static FooterSchemaElement ReadSchemaElement(ref ThriftCompactReader reader, ThriftType element, ModelCharge charge)
        {
            ExpectStruct(element);
            reader.EnterStruct();
            ParquetPhysicalType? physical = null;
            var repetition = ParquetRepetition.Required;
            string? name = null;
            var children = 0;
            var mapOrList = false;
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var type))
            {
                switch (id)
                {
                    case 1:
                        var value = reader.ReadI32(type);
                        if (value < 0 || value > 7)
                        {
                            throw new ThriftFormatException($"Unknown physical type {value}");
                        }
                        physical = (ParquetPhysicalType)value;
                        break;
                    case 3:
                        var rep = reader.ReadI32(type);
                        if (rep < 0 || rep > 2)
                        {
                            throw new ThriftFormatException($"Unknown repetition {rep}");
                        }
                        repetition = (ParquetRepetition)rep;
                        break;
                    case 4:
                        var raw = reader.ReadBinary(type);
                        // An element and its list slot, its name at two bytes per UTF-8 byte at most
                        charge.Add(ElementBytes + 2L * raw.Length);
                        name = Encoding.UTF8.GetString(raw);
                        break;
                    case 5:
                        children = reader.ReadI32(type);
                        if (children < 0 || children > MaxSchemaElements)
                        {
                            throw new ThriftFormatException($"A schema element with {children} children");
                        }
                        break;
                    case 6:
                        // MAP = 1, MAP_KEY_VALUE = 2, LIST = 3
                        var converted = reader.ReadI32(type);
                        mapOrList |= converted is 1 or 2 or 3;
                        break;
                    case 10:
                        mapOrList |= ReadLogicalTypeIsMapOrList(ref reader, type);
                        break;
                    default:
                        reader.Skip(type);
                        break;
                }
            }
            reader.LeaveStruct();
            return new FooterSchemaElement() { Type = physical, Repetition = repetition, Name = name ?? throw new ThriftFormatException("A schema element has no name"), Children = children, IsMapOrList = mapOrList };
        }

        // LogicalType is a union: MAP is field 2, LIST is field 3
        private static bool ReadLogicalTypeIsMapOrList(ref ThriftCompactReader reader, ThriftType type)
        {
            if (type != ThriftType.Struct)
            {
                throw new ThriftFormatException("A logical type is not a struct");
            }
            reader.EnterStruct();
            var result = false;
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var fieldType))
            {
                result |= id is 2 or 3;
                reader.Skip(fieldType);
            }
            reader.LeaveStruct();
            return result;
        }

        private static long ReadRowGroup(ref ThriftCompactReader reader, bool[] projected, out FooterChunk?[] chunks, ref long totalChunks, ModelCharge charge)
        {
            reader.EnterStruct();
            long rows = -1;
            chunks = Array.Empty<FooterChunk?>();
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var type))
            {
                switch (id)
                {
                    case 1:
                        var count = reader.ReadListHeader(type, out var element);
                        ExpectStruct(element);
                        if (count != projected.Length)
                        {
                            throw new ThriftFormatException($"A row group has {count} column chunks for {projected.Length} leaves");
                        }
                        // The chunk array and the group's slots in the model's lists
                        charge.Add(64 + 8L * count);
                        chunks = new FooterChunk?[count];
                        for (int c = 0; c < count; c++)
                        {
                            chunks[c] = ReadColumnChunk(ref reader, projected[c], charge);
                        }
                        totalChunks += count;
                        break;
                    case 3:
                        rows = reader.ReadI64(type);
                        break;
                    default:
                        reader.Skip(type);
                        break;
                }
            }
            reader.LeaveStruct();
            if (rows < 0)
            {
                throw new ThriftFormatException("A row group has no row count");
            }
            if (chunks.Length != projected.Length)
            {
                throw new ThriftFormatException("A row group has no column chunks");
            }
            return rows;
        }

        private static FooterChunk? ReadColumnChunk(ref ThriftCompactReader reader, bool keep, ModelCharge charge)
        {
            reader.EnterStruct();
            FooterChunk? chunk = null;
            var pageIndex = false;
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var type))
            {
                switch (id)
                {
                    case 1:
                        reader.ReadBinary(type);
                        throw new ThriftFormatException("A column chunk is in another file");
                    case 3:
                        chunk = ReadColumnMetaData(ref reader, type, keep, charge);
                        break;
                    case 4:
                    case 6:
                        pageIndex = true;
                        reader.Skip(type);
                        break;
                    case 8:
                    case 9:
                        throw new ThriftFormatException("A column chunk is encrypted");
                    default:
                        reader.Skip(type);
                        break;
                }
            }
            reader.LeaveStruct();
            if (keep && chunk == null)
            {
                throw new ThriftFormatException("A projected column chunk has no metadata");
            }
            if (chunk != null)
            {
                chunk.HasPageIndex = pageIndex;
            }
            return keep ? chunk : null;
        }

        private static FooterChunk? ReadColumnMetaData(ref ThriftCompactReader reader, ThriftType type, bool keep, ModelCharge charge)
        {
            if (!keep)
            {
                reader.Skip(type);
                return null;
            }
            if (type != ThriftType.Struct)
            {
                throw new ThriftFormatException("Column metadata is not a struct");
            }
            reader.EnterStruct();
            var codec = -1;
            int[]? encodings = null;
            long numValues = -1, uncompressed = -1, compressed = -1, dataPage = -1;
            long? dictionaryPage = null;
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var fieldType))
            {
                switch (id)
                {
                    case 2:
                        var count = reader.ReadListHeader(fieldType, out var element);
                        charge.Add(ChunkBytes + 4L * count);
                        encodings = new int[count];
                        for (int i = 0; i < count; i++)
                        {
                            encodings[i] = reader.ReadI32(element);
                        }
                        break;
                    case 4:
                        codec = reader.ReadI32(fieldType);
                        break;
                    case 5:
                        numValues = reader.ReadI64(fieldType);
                        break;
                    case 6:
                        uncompressed = reader.ReadI64(fieldType);
                        break;
                    case 7:
                        compressed = reader.ReadI64(fieldType);
                        break;
                    case 9:
                        dataPage = reader.ReadI64(fieldType);
                        break;
                    case 11:
                        dictionaryPage = reader.ReadI64(fieldType);
                        break;
                    default:
                        reader.Skip(fieldType);
                        break;
                }
            }
            reader.LeaveStruct();
            if (codec < 0 || encodings == null || numValues < 0 || uncompressed < 0 || compressed < 0 || dataPage < 0)
            {
                throw new ThriftFormatException("Column metadata misses a required field or holds a negative size");
            }
            return new FooterChunk()
            {
                Codec = codec,
                Encodings = encodings,
                NumValues = numValues,
                TotalUncompressedSize = uncompressed,
                TotalCompressedSize = compressed,
                DataPageOffset = dataPage,
                DictionaryPageOffset = dictionaryPage
            };
        }

        private static (string Key, string? Value, long Bytes) ReadKeyValue(ref ThriftCompactReader reader, ModelCharge charge)
        {
            reader.EnterStruct();
            string? key = null;
            string? value = null;
            long bytes = 0;
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var type))
            {
                if (id == 1 || id == 2)
                {
                    var raw = reader.ReadBinary(type);
                    bytes += raw.Length;
                    // Only our own key is materialized
                    if (id == 1)
                    {
                        key = raw.SequenceEqual(MarkerKeyBytes) ? MarkerKey : "";
                    }
                    else if (key == MarkerKey && raw.Length <= MaxMarkerBytes)
                    {
                        charge.Add(2L * raw.Length);
                        value = Encoding.UTF8.GetString(raw);
                    }
                }
                else
                {
                    reader.Skip(type);
                }
            }
            reader.LeaveStruct();
            return (key ?? throw new ThriftFormatException("A key-value pair has no key"), value, bytes);
        }

        private static readonly byte[] MarkerKeyBytes = Encoding.UTF8.GetBytes(MarkerKey);
    }
}
