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

using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using ParquetSharp.Arrow;
using DeltaArrayType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.ArrayType;
using DeltaDecimalType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.DecimalType;
using DeltaMapType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.MapType;
using DeltaStructType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.StructType;
using StructField = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.StructField;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    // Binds a data file's columns to the table's fields by name
    internal sealed class FileBinding
    {
        private readonly Node[] _columns;

        private FileBinding(Node[] columns, int[] leafColumns, IReadOnlyList<TimeUnit> timestampUnits, IReadOnlyList<PrecisionLeaf> precisionLeaves)
        {
            _columns = columns;
            LeafColumns = leafColumns;
            TimestampUnits = timestampUnits;
            PrecisionLeaves = precisionLeaves;
        }

        // The leaves to read, every leaf of each bound column
        public int[] LeafColumns { get; }

        // The file's unit per table timestamp leaf, in schema order
        public IReadOnlyList<TimeUnit> TimestampUnits { get; }

        // INT96 and nanosecond leaves the copy must check
        public IReadOnlyList<PrecisionLeaf> PrecisionLeaves { get; }

        // In id mode a field's id under another name is an error
        public static FileBinding Create(DeltaStructType table, ParquetDataFile file, string tableName, bool byId = false)
        {
            var context = new Context(tableName, file, byId);
            var fileFields = file.Manifest.SchemaFields.Select(x => new FileField(x)).ToArray();
            var columns = new Node[table.Fields.Count];
            var bound = new SortedSet<int>();
            for (int i = 0; i < table.Fields.Count; i++)
            {
                var field = table.Fields[i];
                // Top-level columns never fall back to the logical name
                var index = context.FindOne(fileFields, field.PhysicalName ?? field.Name, field.Name);
                columns[i] = index < 0 ? context.Missing(field, fileFields, field.Name) : context.Bind(field.Type, fileFields[index], field.Name, field.Name, field.Nullable);
                columns[i].Source = index;
                if (index >= 0)
                {
                    bound.Add(index);
                }
            }
            // The batch holds the read columns in file order
            var positions = bound.Select((index, position) => (index, position)).ToDictionary(x => x.index, x => x.position);
            foreach (var column in columns.Where(x => x.Source >= 0))
            {
                column.Source = positions[column.Source];
            }
            var leaves = new List<int>();
            foreach (var index in bound)
            {
                AddLeaves(fileFields[index], leaves);
            }
            return new FileBinding(columns, leaves.ToArray(), context.Units, context.Precision);
        }

        // Views in table order over the batch, the caller disposes both
        public ReboundBatch Rebind(RecordBatch batch, MemoryAllocator allocator)
        {
            var owned = new List<ArrowBuffer>();
            try
            {
                var arrays = new IArrowArray[_columns.Length];
                for (int i = 0; i < _columns.Length; i++)
                {
                    var column = _columns[i];
                    // A leaf column is the batch's own array
                    if (column.Source >= 0 && column.Children == null)
                    {
                        arrays[i] = batch.Column(column.Source);
                        continue;
                    }
                    var data = column.Source < 0 ? NullData(column.Type, batch.Length, allocator, owned) : Rebind(column, batch.Column(column.Source).Data, allocator, owned);
                    arrays[i] = ArrowArrayFactory.BuildArray(data);
                }
                return new ReboundBatch(batch.Length, arrays, owned);
            }
            catch
            {
                Release(owned);
                throw;
            }
        }

        internal static void Release(List<ArrowBuffer> owned)
        {
            foreach (var buffer in owned)
            {
                buffer.Dispose();
            }
            owned.Clear();
        }

        private static ArrayData Rebind(Node node, ArrayData source, MemoryAllocator allocator, List<ArrowBuffer> owned)
        {
            if (node.Children == null)
            {
                return source;
            }
            var children = new ArrayData[node.Children.Length];
            switch (node.Kind)
            {
                case SchemaType.Struct:
                    for (int i = 0; i < children.Length; i++)
                    {
                        var child = node.Children[i];
                        // Children are indexed through the parent's offset
                        children[i] = child.Source < 0 ? NullData(child.Type, source.Offset + source.Length, allocator, owned) : Rebind(child, source.Children[child.Source], allocator, owned);
                    }
                    return new ArrayData(StructOf(node, children), source.Length, source.NullCount, source.Offset, new[] { source.Buffers[0] }, children);
                case SchemaType.Array:
                    // The full values child, the offsets index into it
                    children[0] = Rebind(node.Children[0], source.Children[0], allocator, owned);
                    var element = (ListType)node.Type;
                    return new ArrayData(new ListType(new Field(element.ValueField.Name, children[0].DataType, element.ValueField.IsNullable)), source.Length, source.NullCount, source.Offset, source.Buffers, children);
                default:
                    var entries = source.Children[0];
                    var key = Rebind(node.Children[0], entries.Children[0], allocator, owned);
                    var value = Rebind(node.Children[1], entries.Children[1], allocator, owned);
                    var map = (MapType)node.Type;
                    var type = new MapType(new Field(map.KeyField.Name, key.DataType, false), new Field(map.ValueField.Name, value.DataType, map.ValueField.IsNullable), false);
                    var entriesData = new ArrayData(type.KeyValueType, entries.Length, entries.NullCount, entries.Offset, new[] { entries.Buffers[0] }, new[] { key, value });
                    return new ArrayData(type, source.Length, source.NullCount, source.Offset, source.Buffers, new[] { entriesData });
            }
        }

        private static StructType StructOf(Node node, ArrayData[] children)
        {
            var fields = new List<Field>(children.Length);
            for (int i = 0; i < children.Length; i++)
            {
                fields.Add(new Field(node.Children![i].Name, children[i].DataType, node.Children[i].Nullable));
            }
            return new StructType(fields);
        }

        // An all-null array of the table's type, each buffer owned as made
        private static ArrayData NullData(IArrowType type, int length, MemoryAllocator allocator, List<ArrowBuffer> owned)
        {
            var validity = Zeros(BitUtility.ByteCount(length), allocator, owned);
            switch (type)
            {
                case StructType structType:
                    return new ArrayData(type, length, length, 0, new[] { validity }, structType.Fields.Select(x => NullData(x.DataType, length, allocator, owned)).ToArray());
                case ListType listType:
                    return new ArrayData(type, length, length, 0, new[] { validity, Zeros((length + 1) * sizeof(int), allocator, owned) }, new[] { NullData(listType.ValueDataType, 0, allocator, owned) });
                case MapType mapType:
                    return new ArrayData(type, length, length, 0, new[] { validity, Zeros((length + 1) * sizeof(int), allocator, owned) }, new[] { NullData(mapType.KeyValueType, 0, allocator, owned) });
                case StringType:
                case BinaryType:
                    return new ArrayData(type, length, length, 0, new[] { validity, Zeros((length + 1) * sizeof(int), allocator, owned), ArrowBuffer.Empty });
                case BooleanType:
                    return new ArrayData(type, length, length, 0, new[] { validity, Zeros(BitUtility.ByteCount(length), allocator, owned) });
                case FixedWidthType fixedWidth:
                    return new ArrayData(type, length, length, 0, new[] { validity, Zeros(length * (fixedWidth.BitWidth / 8), allocator, owned) });
                default:
                    throw new NotSupportedException($"Cannot make a null column of type {type.Name}");
            }
        }

        private static ArrowBuffer Zeros(int bytes, MemoryAllocator allocator, List<ArrowBuffer> owned)
        {
            if (bytes == 0)
            {
                return ArrowBuffer.Empty;
            }
            var buffer = new ArrowBuffer.Builder<byte>(bytes).Resize(bytes).Build(allocator);
            owned.Add(buffer);
            return buffer;
        }

        private static void AddLeaves(FileField field, List<int> leaves)
        {
            if (field.ColumnIndex >= 0)
            {
                leaves.Add(field.ColumnIndex);
            }
            foreach (var child in field.Children)
            {
                AddLeaves(child, leaves);
            }
        }

        // The file's schema read once, each manifest access imports it again
        private sealed class FileField
        {
            public FileField(SchemaField field)
            {
                var arrow = field.Field;
                Name = arrow.Name;
                DataType = arrow.DataType;
                ColumnIndex = field.ColumnIndex;
                HasId = arrow.HasMetadata && arrow.Metadata.ContainsKey("PARQUET:field_id");
                FieldId = HasId && int.TryParse(arrow.Metadata["PARQUET:field_id"], out var id) ? id : null;
                Children = field.Children.Select(x => new FileField(x)).ToArray();
            }

            public string Name { get; }

            public IArrowType DataType { get; }

            public int ColumnIndex { get; }

            // Written under column mapping, so the name is physical
            public bool HasId { get; }

            public int? FieldId { get; }

            public FileField[] Children { get; }
        }

        // A table field, where it is in the file and its nested fields
        private sealed class Node
        {
            public Node(IArrowType type, string name, bool nullable, SchemaType kind, Node[]? children)
            {
                Type = type;
                Name = name;
                Nullable = nullable;
                Kind = kind;
                Children = children;
            }

            // The table's type, used for missing fields
            public IArrowType Type { get; }

            public string Name { get; }

            public bool Nullable { get; }

            public SchemaType Kind { get; }

            public Node[]? Children { get; }

            // Index among the source's children, -1 when missing
            public int Source { get; set; } = -1;
        }

        private sealed class Context
        {
            private readonly DeltaTypeArrowTypeVisitor _types = new DeltaTypeArrowTypeVisitor();
            private readonly string _table;
            private readonly ParquetDataFile _file;
            private readonly bool _byId;

            public Context(string table, ParquetDataFile file, bool byId)
            {
                _table = table;
                _file = file;
                _byId = byId;
            }

            public List<TimeUnit> Units { get; } = new List<TimeUnit>();

            public List<PrecisionLeaf> Precision { get; } = new List<PrecisionLeaf>();

            public Node Bind(SchemaBaseType type, FileField file, string name, string path, bool nullable)
            {
                var fileType = file.DataType;
                switch (type)
                {
                    case DeltaStructType structType:
                        {
                            if (fileType is not StructType)
                            {
                                throw Mismatch(path, fileType, type);
                            }
                            var children = new Node[structType.Fields.Count];
                            for (int i = 0; i < children.Length; i++)
                            {
                                var field = structType.Fields[i];
                                var childPath = $"{path}.{field.Name}";
                                var index = FindNested(file.Children, structType.Fields, field, childPath);
                                children[i] = index < 0 ? Missing(field, file.Children, childPath) : Bind(field.Type, file.Children[index], field.Name, childPath, field.Nullable);
                                children[i].Source = index;
                            }
                            return new Node(_types.Visit(type), name, nullable, SchemaType.Struct, children);
                        }
                    case DeltaArrayType arrayType:
                        {
                            if (fileType is not ListType || file.Children.Length != 1)
                            {
                                throw Mismatch(path, fileType, type);
                            }
                            // List elements bind by their role
                            var element = Bind(arrayType.ElementType!, file.Children[0], "element", $"{path}.element", arrayType.ContainsNull);
                            element.Source = 0;
                            return new Node(_types.Visit(type), name, nullable, SchemaType.Array, new[] { element });
                        }
                    case DeltaMapType mapType:
                        {
                            if (fileType is not MapType)
                            {
                                throw Mismatch(path, fileType, type);
                            }
                            // Keys and values bind by their role
                            var entries = file.Children.Length == 1 ? file.Children[0].Children : file.Children;
                            if (entries.Length != 2)
                            {
                                throw Mismatch(path, fileType, type);
                            }
                            var key = Bind(mapType.KeyType, entries[0], "key", $"{path}.key", false);
                            var value = Bind(mapType.ValueType, entries[1], "value", $"{path}.value", mapType.ValueContainsNull);
                            key.Source = 0;
                            value.Source = 1;
                            return new Node(_types.Visit(type), name, nullable, SchemaType.Map, new[] { key, value });
                        }
                    default:
                        CheckLeaf(type, file, path);
                        return new Node(_types.Visit(type), name, nullable, type.Type, null);
                }
            }

            // A missing field reads as nulls, unless it may not be null
            public Node Missing(StructField field, FileField[] fields, string path)
            {
                // Id mode would read it by id, so its values are not missing
                foreach (var file in fields)
                {
                    if (_byId && field.FieldId != null && file.FieldId == field.FieldId)
                    {
                        throw new InvalidOperationException($"Delta table {_table} cannot bind file {_file.Path}: column {path} has field id {field.FieldId}, which the file stores as {file.Name}");
                    }
                }
                if (!field.Nullable)
                {
                    throw new InvalidOperationException($"Delta table {_table} cannot bind file {_file.Path}: the file has no column {path}, which is not nullable");
                }
                AddMissingUnits(field.Type);
                var kind = field.Type.Type;
                return new Node(_types.Visit(field.Type), field.Name, true, kind, null);
            }

            // One field by name, or -1
            public int FindOne(FileField[] fields, string name, string path)
            {
                var found = -1;
                for (int i = 0; i < fields.Length; i++)
                {
                    if (string.Equals(fields[i].Name, name, StringComparison.OrdinalIgnoreCase))
                    {
                        if (found >= 0)
                        {
                            throw Ambiguous(path, fields[found].Name, fields[i].Name);
                        }
                        found = i;
                    }
                }
                return found;
            }

            // Physical name first, then a unique logical name no sibling owns
            private int FindNested(FileField[] fields, IReadOnlyList<StructField> siblings, StructField field, string path)
            {
                var physical = FindOne(fields, field.PhysicalName ?? field.Name, path);
                var logical = -1;
                if (field.PhysicalName != null)
                {
                    for (int i = 0; i < fields.Length; i++)
                    {
                        var name = fields[i].Name;
                        // A field with an id may be a dropped one
                        if (i == physical || fields[i].HasId || !string.Equals(name, field.Name, StringComparison.OrdinalIgnoreCase) || IsSiblingPhysical(siblings, field, name))
                        {
                            continue;
                        }
                        if (logical >= 0)
                        {
                            throw Ambiguous(path, fields[logical].Name, name);
                        }
                        logical = i;
                    }
                }
                if (physical >= 0 && logical >= 0)
                {
                    throw Ambiguous(path, fields[physical].Name, fields[logical].Name);
                }
                return physical >= 0 ? physical : logical;
            }

            private static bool IsSiblingPhysical(IReadOnlyList<StructField> siblings, StructField field, string name)
            {
                foreach (var sibling in siblings)
                {
                    if (!ReferenceEquals(sibling, field) && string.Equals(sibling.PhysicalName ?? sibling.Name, name, StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
                return false;
            }

            private void CheckLeaf(SchemaBaseType type, FileField file, string path)
            {
                var fileType = file.DataType;
                var matches = type switch
                {
                    DeltaDecimalType decimalType => fileType is Decimal128Type fileDecimal && fileDecimal.Scale == decimalType.Scale,
                    Schema.Types.TimestampType => fileType is TimestampType,
                    _ => _types.Visit(type).TypeId == fileType.TypeId
                };
                if (!matches)
                {
                    throw Mismatch(path, fileType, type);
                }
                if (fileType is TimestampType timestamp)
                {
                    Units.Add(timestamp.Unit);
                    var column = _file.LowLevel.FileMetaData.Schema.Column(file.ColumnIndex);
                    var int96 = column.PhysicalType == ParquetSharp.PhysicalType.Int96;
                    if (int96 || timestamp.Unit == TimeUnit.Nanosecond)
                    {
                        Precision.Add(new PrecisionLeaf(file.ColumnIndex, int96, column.MaxDefinitionLevel, column.MaxRepetitionLevel));
                    }
                }
            }

            // Missing timestamps keep the default unit
            private void AddMissingUnits(SchemaBaseType type)
            {
                switch (type)
                {
                    case Schema.Types.TimestampType:
                        Units.Add(TimeUnit.Millisecond);
                        break;
                    case DeltaStructType structType:
                        foreach (var field in structType.Fields)
                        {
                            AddMissingUnits(field.Type);
                        }
                        break;
                    case DeltaArrayType arrayType:
                        AddMissingUnits(arrayType.ElementType!);
                        break;
                    case DeltaMapType mapType:
                        AddMissingUnits(mapType.KeyType);
                        AddMissingUnits(mapType.ValueType);
                        break;
                }
            }

            private InvalidOperationException Mismatch(string path, IArrowType fileType, SchemaBaseType tableType)
            {
                return new InvalidOperationException($"Delta table {_table} cannot bind file {_file.Path}: column {path} is {Describe(fileType)} in the file and {Describe(tableType)} in the table");
            }

            private static string Describe(IArrowType type)
            {
                return type switch
                {
                    Decimal128Type decimalType => $"decimal({decimalType.Precision},{decimalType.Scale})",
                    TimestampType timestamp => $"timestamp({timestamp.Unit})",
                    _ => type.Name
                };
            }

            private static string Describe(SchemaBaseType type)
            {
                return type is DeltaDecimalType decimalType ? $"decimal({decimalType.Precision},{decimalType.Scale})" : type.Type.ToString().ToLowerInvariant();
            }

            private InvalidOperationException Ambiguous(string path, string first, string second)
            {
                return new InvalidOperationException($"Delta table {_table} cannot bind file {_file.Path}: column {path} matches both {first} and {second}");
            }
        }
    }

    // Arrays in table order, views over a source batch
    // Leaves keep the file's types, such as timestamp units
    internal sealed class ReboundBatch : IDisposable
    {
        private readonly IArrowArray[] _arrays;
        private readonly List<ArrowBuffer> _owned;

        public ReboundBatch(int length, IArrowArray[] arrays, List<ArrowBuffer> owned)
        {
            Length = length;
            _arrays = arrays;
            _owned = owned;
        }

        public int Length { get; }

        public IArrowArray Column(int index) => _arrays[index];

        // Releases only the buffers the binding made
        public void Dispose()
        {
            FileBinding.Release(_owned);
        }
    }
}
