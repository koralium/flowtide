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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded
{
    internal enum DataNodeKind : byte
    {
        Struct,
        List,
        Map,
        Leaf
    }

    // How a leaf's decoded Arrow values are stored
    internal enum LeafShape : byte
    {
        // Width bytes per value
        Fixed,
        // A bit per value
        Bits,
        // Offsets and value bytes
        Bytes
    }

    // One node of a data file's schema, levels counted from the root
    internal sealed class DataNode
    {
        public required string Name { get; init; }

        public required string Path { get; init; }

        public required DataNodeKind Kind { get; init; }

        public required FooterSchemaElement Element { get; init; }

        public int MaxDefinition { get; init; }

        public int MaxRepetition { get; init; }

        // Under a list or map, so its length is entries, not rows
        public bool Repeated { get; init; }

        public DataNode[] Children { get; set; } = Array.Empty<DataNode>();

        // A leaf's column index
        public int Leaf { get; init; } = -1;

        // Leaves under the node are [FirstLeaf, FirstLeaf + LeafCount)
        public int FirstLeaf { get; set; }

        public int LeafCount { get; set; }

        public LeafShape Shape { get; init; }

        public int Width { get; init; }

        // Arrow converts by copy, keeping its reader's values of PhysicalWidth
        public bool Copied { get; init; }

        public int PhysicalWidth { get; init; }
    }

    // A data file's schema as a tree of the shapes we model
    internal sealed class DataFileSchema
    {
        // Deeper than any table we write, shallow enough for recursion
        public const int MaxDepth = 64;

        private DataFileSchema(DataNode[] columns, DataNode[] leaves)
        {
            Columns = columns;
            Leaves = leaves;
        }

        public IReadOnlyList<DataNode> Columns { get; }

        // Leaves by column index
        public IReadOnlyList<DataNode> Leaves { get; }

        public static DataFileSchema Build(IReadOnlyList<FooterSchemaElement> elements)
        {
            if (elements.Count == 0 || elements[0].Type != null)
            {
                throw new ThriftFormatException("The schema has no root group");
            }
            // Arrow checks the root's annotations as any group's
            if (elements[0].Logical.Kind != LogicalKind.None || elements[0].Converted != null)
            {
                throw new ThriftFormatException("The schema's root has an annotation", CompactionReason.BindingPreflight);
            }
            var position = 1;
            var leaves = new List<DataNode>();
            var columns = new DataNode[elements[0].Children];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Walk(elements, ref position, "", 0, 0, false, leaves, 1);
            }
            if (position != elements.Count)
            {
                throw new ThriftFormatException("The schema's child counts do not cover its elements");
            }
            return new DataFileSchema(columns, leaves.ToArray());
        }

        private static DataNode Walk(IReadOnlyList<FooterSchemaElement> elements, ref int position, string prefix, int definition, int repetition, bool repeated, List<DataNode> leaves, int depth)
        {
            if (position >= elements.Count)
            {
                throw new ThriftFormatException("The schema's child counts run past its elements");
            }
            if (depth > MaxDepth)
            {
                throw new ThriftFormatException($"The schema nests deeper than {MaxDepth}", CompactionReason.MetadataBound);
            }
            var element = elements[position++];
            var path = Join(prefix, element.Name);
            // Arrow's schema import cuts a name at a NUL
            if (element.Name.Contains('\0'))
            {
                throw Unsupported(path, "has a NUL in its name");
            }
            if (element.Repetition == ParquetRepetition.Repeated)
            {
                throw Unsupported(path, "is repeated outside a list or map");
            }
            if (element.Repetition == ParquetRepetition.Optional)
            {
                definition++;
            }
            if (element.Type != null)
            {
                if (element.Children != 0)
                {
                    throw new ThriftFormatException($"Leaf {path} has children");
                }
                var (shape, width, copied, physical) = Decoded(element, path);
                var leaf = new DataNode() { Name = element.Name, Path = path, Kind = DataNodeKind.Leaf, Element = element, MaxDefinition = definition, MaxRepetition = repetition, Repeated = repeated, Leaf = leaves.Count, Shape = shape, Width = width, Copied = copied, PhysicalWidth = physical };
                leaf.FirstLeaf = leaves.Count;
                leaf.LeafCount = 1;
                leaves.Add(leaf);
                return leaf;
            }
            var first = leaves.Count;
            DataNode node;
            if (element.Logical.Kind is not (LogicalKind.None or LogicalKind.List or LogicalKind.Map))
            {
                throw Unsupported(path, $"is a group with logical type {element.Logical.Kind}");
            }
            if (element.IsMapOrList)
            {
                node = Collection(elements, ref position, element, path, definition, repetition, repeated, leaves, depth);
            }
            else
            {
                if (element.Children == 0)
                {
                    throw Unsupported(path, "is an empty group");
                }
                // Arrow refuses a struct's converted type
                if (element.Converted != null)
                {
                    throw Unsupported(path, $"is a struct with converted type {element.Converted}");
                }
                node = new DataNode() { Name = element.Name, Path = path, Kind = DataNodeKind.Struct, Element = element, MaxDefinition = definition, MaxRepetition = repetition, Repeated = repeated };
                var children = new DataNode[element.Children];
                for (int i = 0; i < children.Length; i++)
                {
                    children[i] = Walk(elements, ref position, path, definition, repetition, repeated, leaves, depth + 1);
                }
                node.Children = children;
            }
            node.FirstLeaf = first;
            node.LeafCount = leaves.Count - first;
            return node;
        }

        // LIST > repeated group > element, or MAP > repeated group > key, value
        private static DataNode Collection(IReadOnlyList<FooterSchemaElement> elements, ref int position, FooterSchemaElement element, string path, int definition, int repetition, bool repeated, List<DataNode> leaves, int depth)
        {
            // The converted type counts only without a logical type
            var converted = element.Logical.Kind == LogicalKind.None ? element.Converted : null;
            var list = element.Logical.Kind == LogicalKind.List || converted == 3;
            var map = element.Logical.Kind == LogicalKind.Map || converted == 1;
            if (list == map || element.Children != 1 || position >= elements.Count)
            {
                throw Unsupported(path, "is not a three-level list or map");
            }
            var wrapper = elements[position];
            if (wrapper.Repetition != ParquetRepetition.Repeated || wrapper.Type != null || wrapper.Children != (list ? 1 : 2) || (wrapper.IsMapOrList && !(map && wrapper.Converted == 2)))
            {
                throw Unsupported(path, "is not a three-level list or map");
            }
            // Arrow refuses other annotations on the repeated group
            if (wrapper.Logical.Kind is not (LogicalKind.None or LogicalKind.List or LogicalKind.Map) || (wrapper.Logical.Kind == LogicalKind.None && wrapper.Converted is not (null or 1 or 2 or 3)))
            {
                throw Unsupported(path, "has an annotated list or map wrapper");
            }
            // Arrow reads these legacy wrappers as the element itself
            if (list && (wrapper.Name == "array" || wrapper.Name == element.Name + "_tuple"))
            {
                throw Unsupported(path, $"has a legacy list wrapper {wrapper.Name}");
            }
            position++;
            var inner = Join(path, wrapper.Name);
            var node = new DataNode() { Name = element.Name, Path = path, Kind = list ? DataNodeKind.List : DataNodeKind.Map, Element = element, MaxDefinition = definition, MaxRepetition = repetition, Repeated = repeated };
            var children = new DataNode[wrapper.Children];
            for (int i = 0; i < children.Length; i++)
            {
                children[i] = Walk(elements, ref position, inner, definition + 1, repetition + 1, true, leaves, depth + 1);
            }
            if (map && children[0].Element.Repetition != ParquetRepetition.Required)
            {
                throw Unsupported(path, "has an optional map key");
            }
            node.Children = children;
            return node;
        }

        // The decoded width, and the reader's width when Arrow copies
        private static (LeafShape Shape, int Width, bool Copied, int Physical) Decoded(FooterSchemaElement element, string path)
        {
            var logical = element.Logical;
            var converted = logical.Kind == LogicalKind.None ? element.Converted : null;
            var physical = element.Type switch
            {
                ParquetPhysicalType.Boolean => 1,
                ParquetPhysicalType.Int32 or ParquetPhysicalType.Float => 4,
                ParquetPhysicalType.Int96 => 12,
                ParquetPhysicalType.FixedLenByteArray => element.TypeLength > 0 ? element.TypeLength : throw new ThriftFormatException($"Leaf {path} has no type length", CompactionReason.MissingField),
                ParquetPhysicalType.ByteArray => 0,
                _ => 8
            };
            if (logical.Kind == LogicalKind.Decimal || converted == 5)
            {
                return (LeafShape.Fixed, 16, true, physical);
            }
            // Arrow copies all but plain INT32, INT64, FLOAT, DOUBLE and timestamps
            var unsigned = logical.Kind == LogicalKind.Integer && !logical.Signed;
            return element.Type switch
            {
                // A null column keeps everything the reader decoded
                _ when logical.Kind == LogicalKind.Unknown => (LeafShape.Fixed, 0, true, physical),
                ParquetPhysicalType.Boolean => (LeafShape.Bits, 0, true, 1),
                ParquetPhysicalType.Int32 when logical.Kind == LogicalKind.Integer && logical.Bits < 32 => (LeafShape.Fixed, logical.Bits / 8, true, 4),
                ParquetPhysicalType.Int32 when converted is 15 or 11 => (LeafShape.Fixed, 1, true, 4),
                ParquetPhysicalType.Int32 when converted is 16 or 12 => (LeafShape.Fixed, 2, true, 4),
                ParquetPhysicalType.Int32 when unsigned || logical.Kind is LogicalKind.Date or LogicalKind.Time || converted is 6 or 7 or 13 => (LeafShape.Fixed, 4, true, 4),
                ParquetPhysicalType.Int64 when unsigned || logical.Kind == LogicalKind.Time || converted is 8 or 14 => (LeafShape.Fixed, 8, true, 8),
                ParquetPhysicalType.Int96 => (LeafShape.Fixed, 8, true, 12),
                ParquetPhysicalType.FixedLenByteArray when logical.Kind == LogicalKind.Float16 => (LeafShape.Fixed, 2, false, 0),
                ParquetPhysicalType.FixedLenByteArray => (LeafShape.Fixed, physical, false, 0),
                ParquetPhysicalType.ByteArray => (LeafShape.Bytes, 0, false, 0),
                _ => (LeafShape.Fixed, physical, false, physical)
            };
        }

        // Paths are for messages, so a crafted name cannot make them large
        private static string Join(string prefix, string name)
        {
            var part = name.Length > 64 ? name.Substring(0, 64) + "..." : name;
            var path = prefix.Length == 0 ? part : prefix + "." + part;
            return path.Length > 256 ? path.Substring(0, 256) + "..." : path;
        }

        private static ThriftFormatException Unsupported(string path, string detail)
        {
            return new ThriftFormatException($"Column {path} {detail}", CompactionReason.BindingPreflight);
        }
    }
}
