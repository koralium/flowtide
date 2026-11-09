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

using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using DeltaArrayType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.ArrayType;
using DeltaDecimalType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.DecimalType;
using DeltaMapType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.MapType;
using DeltaStructType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.StructType;
using StructField = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.StructField;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded
{
    // A table node and the file node it reads, none when synthesized
    internal sealed class PlanNode
    {
        public required string Path { get; init; }

        public required SchemaBaseType Type { get; init; }

        public DataNode? Source { get; init; }

        public PlanNode[] Children { get; init; } = Array.Empty<PlanNode>();

        public LeafShape Shape { get; init; }

        public int Width { get; init; }

        // Lists, maps, strings and binaries keep offsets
        public bool HasOffsets => Type.Type is SchemaType.Array or SchemaType.Map or SchemaType.String or SchemaType.Binary;

        public bool IsLeaf => Type.Type is not (SchemaType.Struct or SchemaType.Array or SchemaType.Map);
    }

    // How FileBinding will bind one file, worked out from its footer alone
    internal sealed class BindingPlan
    {
        public required PlanNode[] Columns { get; init; }

        // Every leaf of each bound top-level column, as FileBinding reads them
        public required bool[] Read { get; init; }

        public required IReadOnlyList<TimeUnit> Units { get; init; }

        public required IReadOnlyList<PrecisionLeaf> Precision { get; init; }

        public required IReadOnlyList<string> MissingPaths { get; init; }
    }

    // FileBinding's matching over the footer, declining before any native open
    internal static class BindingPreflight
    {
        public static BindingPlan Plan(DeltaStructType table, DataFileSchema file, bool byId)
        {
            // Arrow converts every leaf at open, read or not
            var unread = file.Leaves.FirstOrDefault(x => ArrowType(x.Element, out _, out _) == null && !ArrowOnly(x.Element));
            if (unread != null)
            {
                throw Declined($"leaf {unread.Path} has a type Arrow may refuse");
            }
            var context = new Context(byId);
            var columns = new PlanNode[table.Fields.Count];
            var read = new bool[file.Leaves.Count];
            for (int i = 0; i < columns.Length; i++)
            {
                var field = table.Fields[i];
                // Top-level columns never fall back to the logical name
                var index = FindOne(file.Columns, field.PhysicalName ?? field.Name, field.Name);
                if (index < 0)
                {
                    columns[i] = context.Missing(field, file.Columns, field.Name);
                    continue;
                }
                var column = file.Columns[index];
                columns[i] = context.Bind(field.Type, column, field.Name);
                for (int leaf = column.FirstLeaf; leaf < column.FirstLeaf + column.LeafCount; leaf++)
                {
                    read[leaf] = true;
                }
            }
            return new BindingPlan() { Columns = columns, Read = read, Units = context.Units, Precision = context.Precision, MissingPaths = context.MissingPaths };
        }

        // The Arrow type the reader gives a leaf, or null to decline
        public static ArrowTypeId? ArrowType(FooterSchemaElement leaf, out TimeUnit unit, out int scale)
        {
            unit = TimeUnit.Millisecond;
            scale = 0;
            var logical = leaf.Logical;
            if (logical.Kind == LogicalKind.Decimal || (logical.Kind == LogicalKind.None && leaf.Converted == 5))
            {
                var precision = logical.Kind == LogicalKind.Decimal ? logical.Precision : leaf.Precision;
                scale = logical.Kind == LogicalKind.Decimal ? logical.Scale : leaf.Scale;
                // Arrow refuses these, byte arrays by their values
                var fits = leaf.Type switch
                {
                    ParquetPhysicalType.Int32 => precision <= 9,
                    ParquetPhysicalType.Int64 => precision <= 18,
                    ParquetPhysicalType.FixedLenByteArray => leaf.TypeLength is >= 1 and <= 16 && precision <= (int)Math.Floor(Math.Log10(2) * (8.0 * leaf.TypeLength - 1)),
                    _ => false
                };
                if (!fits || precision < 1 || precision > 38 || scale < 0 || scale > precision)
                {
                    return null;
                }
                return ArrowTypeId.Decimal128;
            }
            if (logical.Kind == LogicalKind.Timestamp || (logical.Kind == LogicalKind.None && leaf.Converted is 9 or 10))
            {
                unit = logical.Kind == LogicalKind.Timestamp
                    ? logical.Unit switch { 1 => TimeUnit.Millisecond, 2 => TimeUnit.Microsecond, _ => TimeUnit.Nanosecond }
                    : leaf.Converted == 9 ? TimeUnit.Millisecond : TimeUnit.Microsecond;
                return leaf.Type == ParquetPhysicalType.Int64 ? ArrowTypeId.Timestamp : null;
            }
            switch (leaf.Type)
            {
                case ParquetPhysicalType.Boolean when logical.Kind == LogicalKind.None && leaf.Converted == null:
                    return ArrowTypeId.Boolean;
                case ParquetPhysicalType.Int32:
                case ParquetPhysicalType.Int64:
                    return Integer(leaf, leaf.Type == ParquetPhysicalType.Int64);
                case ParquetPhysicalType.Int96 when logical.Kind == LogicalKind.None && leaf.Converted == null:
                    // OpenLocal coerces INT96 to microseconds
                    unit = TimeUnit.Microsecond;
                    return ArrowTypeId.Timestamp;
                case ParquetPhysicalType.Float when logical.Kind == LogicalKind.None && leaf.Converted == null:
                    return ArrowTypeId.Float;
                case ParquetPhysicalType.Double when logical.Kind == LogicalKind.None && leaf.Converted == null:
                    return ArrowTypeId.Double;
                case ParquetPhysicalType.ByteArray:
                    return logical.Kind switch
                    {
                        LogicalKind.String or LogicalKind.Json => ArrowTypeId.String,
                        LogicalKind.Enum or LogicalKind.Bson => ArrowTypeId.Binary,
                        LogicalKind.None => leaf.Converted switch
                        {
                            null => ArrowTypeId.Binary,
                            0 or 19 => ArrowTypeId.String,
                            4 or 20 => ArrowTypeId.Binary,
                            _ => null
                        },
                        _ => null
                    };
                default:
                    return null;
            }
        }

        // Valid leaves no table type binds: unsigned, time, fixed bytes
        private static bool ArrowOnly(FooterSchemaElement leaf)
        {
            var logical = leaf.Logical;
            var converted = logical.Kind == LogicalKind.None ? leaf.Converted : null;
            var plain = logical.Kind is LogicalKind.None or LogicalKind.Empty && converted == null;
            var unsigned = logical.Kind == LogicalKind.Integer && !logical.Signed;
            // Arrow reads empty types by their physical type, null ones but byte arrays
            if (logical.Kind is LogicalKind.Empty or LogicalKind.Unknown)
            {
                return logical.Kind == LogicalKind.Empty || leaf.Type is not (ParquetPhysicalType.ByteArray or ParquetPhysicalType.FixedLenByteArray);
            }
            return leaf.Type switch
            {
                ParquetPhysicalType.Int32 => (unsigned && logical.Bits <= 32) || (logical.Kind == LogicalKind.Time && logical.Unit == 1) || converted is 7 or 11 or 12 or 13,
                ParquetPhysicalType.Int64 => (unsigned && logical.Bits == 64) || (logical.Kind == LogicalKind.Time && logical.Unit is 2 or 3) || converted is 8 or 14,
                ParquetPhysicalType.FixedLenByteArray => plain || (logical.Kind == LogicalKind.Float16 && leaf.TypeLength == 2) || (logical.Kind == LogicalKind.Uuid && leaf.TypeLength == 16) || (converted == 21 && leaf.TypeLength == 12),
                _ => false
            };
        }

        // INT32 and INT64 by their INTEGER, DATE or legacy annotation
        private static ArrowTypeId? Integer(FooterSchemaElement leaf, bool wide)
        {
            var logical = leaf.Logical;
            if (logical.Kind == LogicalKind.Integer)
            {
                return (logical.Bits, logical.Signed) switch
                {
                    (8, true) when !wide => ArrowTypeId.Int8,
                    (16, true) when !wide => ArrowTypeId.Int16,
                    (32, true) when !wide => ArrowTypeId.Int32,
                    (64, true) when wide => ArrowTypeId.Int64,
                    _ => null
                };
            }
            if (logical.Kind == LogicalKind.Date)
            {
                return wide ? null : ArrowTypeId.Date32;
            }
            if (logical.Kind != LogicalKind.None)
            {
                return null;
            }
            return (leaf.Converted, wide) switch
            {
                (null, false) or (17, false) => ArrowTypeId.Int32,
                (null, true) or (18, true) => ArrowTypeId.Int64,
                (15, false) => ArrowTypeId.Int8,
                (16, false) => ArrowTypeId.Int16,
                (6, false) => ArrowTypeId.Date32,
                _ => null
            };
        }

        private static ArrowTypeId Expected(SchemaBaseType type)
        {
            return type.Type switch
            {
                SchemaType.Boolean => ArrowTypeId.Boolean,
                SchemaType.Byte => ArrowTypeId.Int8,
                SchemaType.Short => ArrowTypeId.Int16,
                SchemaType.Integer => ArrowTypeId.Int32,
                SchemaType.Long => ArrowTypeId.Int64,
                SchemaType.Float => ArrowTypeId.Float,
                SchemaType.Double => ArrowTypeId.Double,
                SchemaType.Date => ArrowTypeId.Date32,
                SchemaType.Timestamp => ArrowTypeId.Timestamp,
                SchemaType.Decimal => ArrowTypeId.Decimal128,
                SchemaType.Binary => ArrowTypeId.Binary,
                _ => ArrowTypeId.String
            };
        }

        // One field by name, case-insensitive, or -1; two matches are ambiguous
        private static int FindOne(IReadOnlyList<DataNode> fields, string name, string path)
        {
            var found = -1;
            for (int i = 0; i < fields.Count; i++)
            {
                if (string.Equals(fields[i].Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    if (found >= 0)
                    {
                        throw Declined($"column {path} matches both {fields[found].Name} and {fields[i].Name}");
                    }
                    found = i;
                }
            }
            return found;
        }

        private static DataFileDeclinedException Declined(string detail)
        {
            return new DataFileDeclinedException(CompactionReason.BindingPreflight, $"The file cannot bind: {detail}");
        }

        private sealed class Context
        {
            private readonly bool _byId;

            public Context(bool byId)
            {
                _byId = byId;
            }

            public List<TimeUnit> Units { get; } = new List<TimeUnit>();

            public List<PrecisionLeaf> Precision { get; } = new List<PrecisionLeaf>();

            public List<string> MissingPaths { get; } = new List<string>();

            public PlanNode Bind(SchemaBaseType type, DataNode file, string path)
            {
                switch (type)
                {
                    case DeltaStructType structType:
                        {
                            if (file.Kind != DataNodeKind.Struct)
                            {
                                throw Declined($"{path} is a struct in the table and a {file.Kind} in the file");
                            }
                            var children = new PlanNode[structType.Fields.Count];
                            for (int i = 0; i < children.Length; i++)
                            {
                                var field = structType.Fields[i];
                                var childPath = $"{path}.{field.Name}";
                                var index = FindNested(file.Children, structType.Fields, field, childPath);
                                children[i] = index < 0 ? Missing(field, file.Children, childPath) : Bind(field.Type, file.Children[index], childPath);
                            }
                            return new PlanNode() { Path = path, Type = type, Source = file, Children = children };
                        }
                    case DeltaArrayType arrayType:
                        {
                            if (file.Kind != DataNodeKind.List)
                            {
                                throw Declined($"{path} is an array in the table and a {file.Kind} in the file");
                            }
                            var element = Bind(arrayType.ElementType!, file.Children[0], $"{path}.element");
                            return new PlanNode() { Path = path, Type = type, Source = file, Children = new[] { element } };
                        }
                    case DeltaMapType mapType:
                        {
                            if (file.Kind != DataNodeKind.Map)
                            {
                                throw Declined($"{path} is a map in the table and a {file.Kind} in the file");
                            }
                            var key = Bind(mapType.KeyType, file.Children[0], $"{path}.key");
                            var value = Bind(mapType.ValueType, file.Children[1], $"{path}.value");
                            return new PlanNode() { Path = path, Type = type, Source = file, Children = new[] { key, value } };
                        }
                    default:
                        {
                            if (file.Kind != DataNodeKind.Leaf)
                            {
                                throw Declined($"{path} is a {type.Type} in the table and a {file.Kind} in the file");
                            }
                            var arrow = ArrowType(file.Element, out var unit, out var scale);
                            var matches = arrow != null && (type is DeltaDecimalType decimalType
                                ? arrow == ArrowTypeId.Decimal128 && scale == decimalType.Scale
                                : arrow == Expected(type));
                            if (!matches)
                            {
                                throw Declined($"{path} is a {type.Type} in the table and {arrow?.ToString() ?? "an unsupported type"} in the file");
                            }
                            if (arrow == ArrowTypeId.Timestamp)
                            {
                                Units.Add(unit);
                                var int96 = file.Element.Type == ParquetPhysicalType.Int96;
                                if (int96 || unit == TimeUnit.Nanosecond)
                                {
                                    Precision.Add(new PrecisionLeaf(file.Leaf, int96, (short)file.MaxDefinition, (short)file.MaxRepetition));
                                }
                            }
                            var (shape, width) = OutputShape.Shape(type);
                            return new PlanNode() { Path = path, Type = type, Source = file, Shape = shape, Width = width };
                        }
                }
            }

            // A missing field reads as nulls, if nullable and not renamed by id
            public PlanNode Missing(StructField field, IReadOnlyList<DataNode> fields, string path)
            {
                foreach (var file in fields)
                {
                    if (_byId && field.FieldId != null && file.Element.FieldId == field.FieldId)
                    {
                        throw Declined($"column {path} has field id {field.FieldId}, which the file stores as {file.Name}");
                    }
                }
                if (!field.Nullable)
                {
                    throw Declined($"the file has no column {path}, which is not nullable");
                }
                MissingPaths.Add(path);
                return Synthesized(field.Type, path);
            }

            // Missing timestamps keep the default unit
            private PlanNode Synthesized(SchemaBaseType type, string path)
            {
                switch (type)
                {
                    case DeltaStructType structType:
                        return new PlanNode() { Path = path, Type = type, Children = structType.Fields.Select(x => Synthesized(x.Type, $"{path}.{x.Name}")).ToArray() };
                    case DeltaArrayType arrayType:
                        return new PlanNode() { Path = path, Type = type, Children = new[] { Synthesized(arrayType.ElementType!, $"{path}.element") } };
                    case DeltaMapType mapType:
                        return new PlanNode() { Path = path, Type = type, Children = new[] { Synthesized(mapType.KeyType, $"{path}.key"), Synthesized(mapType.ValueType, $"{path}.value") } };
                    default:
                        if (type.Type == SchemaType.Timestamp)
                        {
                            Units.Add(TimeUnit.Millisecond);
                        }
                        var (shape, width) = OutputShape.Shape(type);
                        return new PlanNode() { Path = path, Type = type, Shape = shape, Width = width };
                }
            }

            // Physical name first, then a unique logical name no sibling owns
            private static int FindNested(IReadOnlyList<DataNode> fields, IReadOnlyList<StructField> siblings, StructField field, string path)
            {
                var physical = FindOne(fields, field.PhysicalName ?? field.Name, path);
                var logical = -1;
                if (field.PhysicalName != null)
                {
                    for (int i = 0; i < fields.Count; i++)
                    {
                        var name = fields[i].Name;
                        // A field with an id may be a dropped one
                        if (i == physical || fields[i].Element.FieldId != null || !string.Equals(name, field.Name, StringComparison.OrdinalIgnoreCase) || IsSiblingPhysical(siblings, field, name))
                        {
                            continue;
                        }
                        if (logical >= 0)
                        {
                            throw Declined($"{path} matches both {fields[logical].Name} and {name}");
                        }
                        logical = i;
                    }
                }
                if (physical >= 0 && logical >= 0)
                {
                    throw Declined($"{path} matches both {fields[physical].Name} and {fields[logical].Name}");
                }
                return physical >= 0 ? physical : logical;
            }

            private static bool IsSiblingPhysical(IReadOnlyList<StructField> siblings, StructField field, string name)
            {
                return siblings.Any(x => !ReferenceEquals(x, field) && string.Equals(x.PhysicalName ?? x.Name, name, StringComparison.OrdinalIgnoreCase));
            }
        }
    }
}
