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

    // The LogicalType union's members, by meaning
    internal enum LogicalKind : byte
    {
        None,
        String,
        Map,
        List,
        Enum,
        Decimal,
        Date,
        Time,
        Timestamp,
        Integer,
        Unknown,
        Json,
        Bson,
        Uuid,
        Float16,
        Other,
        // Present with no member, Arrow then ignores the converted type
        Empty
    }

    // Unit is 1 for ms, 2 for µs and 3 for ns
    internal readonly record struct LogicalTypeInfo(LogicalKind Kind, int Scale = 0, int Precision = 0, int Unit = 0, bool Utc = false, int Bits = 0, bool Signed = false);

    internal sealed class FooterSchemaElement
    {
        public ParquetPhysicalType? Type { get; init; }

        public ParquetRepetition Repetition { get; init; }

        public required string Name { get; init; }

        public int Children { get; init; }

        // ConvertedType MAP, MAP_KEY_VALUE or LIST, or the matching logical type
        public bool IsMapOrList { get; init; }

        // Kept for data files only
        public int TypeLength { get; init; }

        public int? Converted { get; init; }

        public int Scale { get; init; }

        public int Precision { get; init; }

        public int? FieldId { get; init; }

        public LogicalTypeInfo Logical { get; init; }
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

        // Data files only: the chunk's type and SizeStatistics' unencoded byte-array bytes
        public int PhysicalType { get; init; } = -1;

        public long? UnencodedBytes { get; init; }
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
        public const string ArrowSchemaKey = "ARROW:schema";

        // Longer values are not kept: a marker past this many UTF-8 bytes is unmarked anyway, created_by is informational
        public const int MaxMarkerBytes = 4 * CoverageMarker.MaxLength;
        public const int MaxCreatedByBytes = 256;

        // The managed charge per object: a model element, a projected chunk, and what the projection keeps per schema element
        // An element holds its logical type and ids on both paths
        private const long ElementBytes = 192;
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

        // Data files only
        public long NumRows { get; init; } = -1;

        public bool HasArrowSchema { get; init; }

        /// <summary>
        /// Parses a footer within its limits; once the schema is read, project tells which leaf indices (in file order) keep their chunk fields.
        /// </summary>
        public static ParquetFooterModel Parse(ReadOnlySpan<byte> footer, Func<IReadOnlyList<FooterSchemaElement>, bool[]> project, ModelCharge? charge = null, bool dataFile = false)
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
            long numRows = -1;
            var arrowSchema = false;
            var version = false;
            var columnOrders = -1;
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
                                throw new ThriftFormatException($"{count} schema elements", CompactionReason.MetadataBound);
                            }
                            for (int i = 0; i < count; i++)
                            {
                                schema.Add(ReadSchemaElement(ref reader, element, charge, dataFile));
                            }
                            seen |= 1;
                            break;
                        }
                    case 1 when dataFile:
                        reader.ReadI32(type);
                        version = true;
                        break;
                    case 3 when dataFile:
                        numRows = reader.ReadI64(type);
                        break;
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
                                throw new ThriftFormatException($"{count} row groups", CompactionReason.MetadataBound);
                            }
                            for (int i = 0; i < count; i++)
                            {
                                ExpectStruct(element);
                                rowGroupRows.Add(ReadRowGroup(ref reader, keep, out var rowGroupChunks, ref totalChunks, charge, dataFile));
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
                                arrowSchema |= key == ArrowSchemaKey;
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
                    case 7 when dataFile:
                        {
                            columnOrders = reader.ReadListHeader(type, out var element);
                            // Arrow reads each as a struct, whatever the list says
                            ExpectStruct(element);
                            if (columnOrders > MaxSchemaElements)
                            {
                                throw new ThriftFormatException($"{columnOrders} column orders", CompactionReason.MetadataBound);
                            }
                            for (int i = 0; i < columnOrders; i++)
                            {
                                reader.Skip(ThriftType.Struct);
                            }
                            break;
                        }
                    case 8:
                        throw new ThriftFormatException("The file is encrypted", CompactionReason.Encrypted);
                    default:
                        reader.Skip(type);
                        break;
                }
            }
            reader.LeaveStruct();
            if (seen != 3)
            {
                throw new ThriftFormatException("The footer has no schema or row groups", CompactionReason.MissingField);
            }
            // Arrow refuses a footer without its version or an order per leaf
            if (dataFile && !version)
            {
                throw new ThriftFormatException("The footer has no version", CompactionReason.MissingField);
            }
            if (columnOrders >= 0 && columnOrders != schema.Count(x => x.Type != null))
            {
                throw new ThriftFormatException($"The footer has {columnOrders} column orders for {schema.Count(x => x.Type != null)} leaves");
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
                KeyValueBytes = keyValueBytes,
                NumRows = numRows,
                HasArrowSchema = arrowSchema
            };
        }

        private static void ExpectStruct(ThriftType element)
        {
            if (element != ThriftType.Struct)
            {
                throw new ThriftFormatException($"Expected a list of structs, found {element}");
            }
        }

        private static FooterSchemaElement ReadSchemaElement(ref ThriftCompactReader reader, ThriftType element, ModelCharge charge, bool dataFile)
        {
            ExpectStruct(element);
            reader.EnterStruct();
            ParquetPhysicalType? physical = null;
            var repetition = ParquetRepetition.Required;
            string? name = null;
            var children = 0;
            var mapOrList = false;
            int typeLength = 0, scale = 0, precision = 0;
            int? converted = null, fieldId = null;
            var logical = default(LogicalTypeInfo);
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
                    case 2 when dataFile:
                        typeLength = reader.ReadI32(type);
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
                        converted = reader.ReadI32(type);
                        mapOrList |= converted is 1 or 2 or 3;
                        break;
                    case 7 when dataFile:
                        scale = reader.ReadI32(type);
                        break;
                    case 8 when dataFile:
                        precision = reader.ReadI32(type);
                        break;
                    case 9 when dataFile:
                        // Arrow drops negative field ids
                        var storedId = reader.ReadI32(type);
                        fieldId = storedId >= 0 ? storedId : null;
                        break;
                    case 10 when dataFile:
                        logical = ReadLogicalType(ref reader, type);
                        mapOrList |= logical.Kind is LogicalKind.Map or LogicalKind.List;
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
            return new FooterSchemaElement()
            {
                Type = physical,
                Repetition = repetition,
                Name = name ?? throw new ThriftFormatException("A schema element has no name", CompactionReason.MissingField),
                Children = children,
                IsMapOrList = mapOrList,
                TypeLength = typeLength,
                Converted = dataFile ? converted : null,
                Scale = scale,
                Precision = precision,
                FieldId = fieldId,
                Logical = logical
            };
        }

        // The union's member and its parameters; an unknown member is Other
        private static LogicalTypeInfo ReadLogicalType(ref ThriftCompactReader reader, ThriftType type)
        {
            if (type != ThriftType.Struct)
            {
                throw new ThriftFormatException("A logical type is not a struct");
            }
            reader.EnterStruct();
            var result = new LogicalTypeInfo(LogicalKind.None);
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var fieldType))
            {
                // Thrift skips a member of another wire type
                if (fieldType != ThriftType.Struct)
                {
                    reader.Skip(fieldType);
                    continue;
                }
                if (result.Kind != LogicalKind.None)
                {
                    throw new ThriftFormatException("A logical type sets more than one member");
                }
                result = id switch
                {
                    1 => Empty(ref reader, fieldType, LogicalKind.String),
                    2 => Empty(ref reader, fieldType, LogicalKind.Map),
                    3 => Empty(ref reader, fieldType, LogicalKind.List),
                    4 => Empty(ref reader, fieldType, LogicalKind.Enum),
                    5 => ReadDecimal(ref reader, fieldType),
                    6 => Empty(ref reader, fieldType, LogicalKind.Date),
                    7 => ReadTime(ref reader, fieldType, LogicalKind.Time),
                    8 => ReadTime(ref reader, fieldType, LogicalKind.Timestamp),
                    10 => ReadInteger(ref reader, fieldType),
                    11 => Empty(ref reader, fieldType, LogicalKind.Unknown),
                    12 => Empty(ref reader, fieldType, LogicalKind.Json),
                    13 => Empty(ref reader, fieldType, LogicalKind.Bson),
                    14 => Empty(ref reader, fieldType, LogicalKind.Uuid),
                    15 => Empty(ref reader, fieldType, LogicalKind.Float16),
                    _ => Empty(ref reader, fieldType, LogicalKind.Other)
                };
            }
            reader.LeaveStruct();
            return result.Kind == LogicalKind.None ? new LogicalTypeInfo(LogicalKind.Empty) : result;
        }

        private static LogicalTypeInfo Empty(ref ThriftCompactReader reader, ThriftType type, LogicalKind kind)
        {
            reader.Skip(type);
            return new LogicalTypeInfo(kind);
        }

        private static LogicalTypeInfo ReadDecimal(ref ThriftCompactReader reader, ThriftType type)
        {
            ExpectStruct(type);
            reader.EnterStruct();
            int scale = -1, precision = -1;
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var fieldType))
            {
                switch (id)
                {
                    case 1:
                        scale = reader.ReadI32(fieldType);
                        break;
                    case 2:
                        precision = reader.ReadI32(fieldType);
                        break;
                    default:
                        reader.Skip(fieldType);
                        break;
                }
            }
            reader.LeaveStruct();
            if (scale < 0 || precision < 1)
            {
                throw new ThriftFormatException("A decimal type misses its scale or precision", CompactionReason.MissingField);
            }
            return new LogicalTypeInfo(LogicalKind.Decimal, Scale: scale, Precision: precision);
        }

        // TIME and TIMESTAMP: isAdjustedToUTC and a unit union of empty members
        private static LogicalTypeInfo ReadTime(ref ThriftCompactReader reader, ThriftType type, LogicalKind kind)
        {
            ExpectStruct(type);
            reader.EnterStruct();
            bool? utc = null;
            var unit = 0;
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var fieldType))
            {
                switch (id)
                {
                    case 1:
                        utc = ThriftCompactReader.ReadBool(fieldType);
                        break;
                    case 2:
                        ExpectStruct(fieldType);
                        reader.EnterStruct();
                        short unitLast = 0;
                        ulong unitSeen = 0;
                        while (reader.ReadField(ref unitLast, ref unitSeen, out var unitId, out var unitType))
                        {
                            // Thrift skips a unit member of another wire type
                            if (unitType == ThriftType.Struct)
                            {
                                unit = unit == 0 && unitId is >= 1 and <= 3 ? unitId : -1;
                            }
                            reader.Skip(unitType);
                        }
                        reader.LeaveStruct();
                        break;
                    default:
                        reader.Skip(fieldType);
                        break;
                }
            }
            reader.LeaveStruct();
            if (utc == null || unit <= 0)
            {
                throw new ThriftFormatException($"A {kind} type misses its unit or UTC flag", CompactionReason.MissingField);
            }
            return new LogicalTypeInfo(kind, Unit: unit, Utc: utc.Value);
        }

        private static LogicalTypeInfo ReadInteger(ref ThriftCompactReader reader, ThriftType type)
        {
            ExpectStruct(type);
            reader.EnterStruct();
            var bits = 0;
            bool? signed = null;
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var fieldType))
            {
                switch (id)
                {
                    case 1:
                        bits = reader.ReadI8(fieldType);
                        break;
                    case 2:
                        signed = ThriftCompactReader.ReadBool(fieldType);
                        break;
                    default:
                        reader.Skip(fieldType);
                        break;
                }
            }
            reader.LeaveStruct();
            if (bits is not (8 or 16 or 32 or 64) || signed == null)
            {
                throw new ThriftFormatException($"An integer type of {bits} bits or no sign", CompactionReason.Malformed);
            }
            return new LogicalTypeInfo(LogicalKind.Integer, Bits: bits, Signed: signed.Value);
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

        private static long ReadRowGroup(ref ThriftCompactReader reader, bool[] projected, out FooterChunk?[] chunks, ref long totalChunks, ModelCharge charge, bool dataFile)
        {
            reader.EnterStruct();
            long rows = -1;
            var totalBytes = false;
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
                            chunks[c] = ReadColumnChunk(ref reader, projected[c], charge, dataFile);
                        }
                        totalChunks += count;
                        break;
                    case 2 when dataFile:
                        reader.ReadI64(type);
                        totalBytes = true;
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
            if (rows < 0 || (dataFile && !totalBytes))
            {
                throw new ThriftFormatException("A row group has no row count or byte size", CompactionReason.MissingField);
            }
            if (chunks.Length != projected.Length)
            {
                throw new ThriftFormatException("A row group has no column chunks", CompactionReason.MissingField);
            }
            return rows;
        }

        private static FooterChunk? ReadColumnChunk(ref ThriftCompactReader reader, bool keep, ModelCharge charge, bool dataFile)
        {
            reader.EnterStruct();
            FooterChunk? chunk = null;
            var pageIndex = false;
            var fileOffset = false;
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var type))
            {
                switch (id)
                {
                    case 1:
                        reader.ReadBinary(type);
                        throw new ThriftFormatException("A column chunk is in another file");
                    case 2 when dataFile:
                        reader.ReadI64(type);
                        fileOffset = true;
                        break;
                    case 3:
                        chunk = ReadColumnMetaData(ref reader, type, keep, charge, dataFile);
                        break;
                    case 4:
                    case 6:
                        pageIndex = true;
                        reader.Skip(type);
                        break;
                    case 8:
                    case 9:
                        throw new ThriftFormatException("A column chunk is encrypted", CompactionReason.Encrypted);
                    default:
                        reader.Skip(type);
                        break;
                }
            }
            reader.LeaveStruct();
            if ((keep && chunk == null) || (dataFile && !fileOffset))
            {
                throw new ThriftFormatException("A projected column chunk has no metadata or offset", CompactionReason.MissingField);
            }
            if (chunk != null)
            {
                chunk.HasPageIndex = pageIndex;
            }
            return keep ? chunk : null;
        }

        private static FooterChunk? ReadColumnMetaData(ref ThriftCompactReader reader, ThriftType type, bool keep, ModelCharge charge, bool dataFile)
        {
            // Arrow parses every chunk, so data files check unread ones too
            if (!keep && !dataFile)
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
            var path = !dataFile;
            long numValues = -1, uncompressed = -1, compressed = -1, dataPage = -1;
            long? dictionaryPage = null, unencoded = null;
            var physical = -1;
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var fieldType))
            {
                switch (id)
                {
                    case 1 when dataFile:
                        physical = reader.ReadI32(fieldType);
                        break;
                    case 16 when dataFile:
                        unencoded = ReadUnencodedBytes(ref reader, fieldType);
                        break;
                    case 2:
                        var count = reader.ReadListHeader(fieldType, out var element);
                        if (keep)
                        {
                            charge.Add(ChunkBytes + 4L * count);
                        }
                        encodings = keep ? new int[count] : Array.Empty<int>();
                        for (int i = 0; i < count; i++)
                        {
                            var encoding = reader.ReadI32(element);
                            if (keep)
                            {
                                encodings[i] = encoding;
                            }
                        }
                        break;
                    case 3 when dataFile:
                        var parts = reader.ReadListHeader(fieldType, out var part);
                        for (int i = 0; i < parts; i++)
                        {
                            reader.ReadBinary(part);
                        }
                        path = true;
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
            if (codec < 0 || encodings == null || !path || numValues < 0 || uncompressed < 0 || compressed < 0 || dataPage < 0 || (dataFile && physical < 0))
            {
                throw new ThriftFormatException("Column metadata misses a required field or holds a negative size", CompactionReason.MissingField);
            }
            if (unencoded < 0)
            {
                throw new ThriftFormatException("Size statistics hold a negative byte count");
            }
            if (!keep)
            {
                return null;
            }
            return new FooterChunk()
            {
                Codec = codec,
                Encodings = encodings,
                NumValues = numValues,
                TotalUncompressedSize = uncompressed,
                TotalCompressedSize = compressed,
                DataPageOffset = dataPage,
                DictionaryPageOffset = dictionaryPage,
                PhysicalType = physical,
                UnencodedBytes = unencoded
            };
        }

        // SizeStatistics field 1, the byte-array bytes without length prefixes
        private static long? ReadUnencodedBytes(ref ThriftCompactReader reader, ThriftType type)
        {
            ExpectStruct(type);
            reader.EnterStruct();
            long? bytes = null;
            short last = 0;
            ulong seen = 0;
            while (reader.ReadField(ref last, ref seen, out var id, out var fieldType))
            {
                if (id == 1)
                {
                    bytes = reader.ReadI64(fieldType);
                }
                else
                {
                    reader.Skip(fieldType);
                }
            }
            reader.LeaveStruct();
            return bytes;
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
                        key = raw.SequenceEqual(MarkerKeyBytes) ? MarkerKey : raw.SequenceEqual(ArrowSchemaKeyBytes) ? ArrowSchemaKey : "";
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
        private static readonly byte[] ArrowSchemaKeyBytes = Encoding.UTF8.GetBytes(ArrowSchemaKey);
    }
}
