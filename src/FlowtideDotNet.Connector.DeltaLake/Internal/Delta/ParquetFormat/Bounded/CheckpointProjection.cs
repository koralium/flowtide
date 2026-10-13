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
using System.Collections.Immutable;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded
{
    internal enum ProjectedRole : byte
    {
        // A field our checkpoints write
        Field,
        SidecarPath,
        StatsNumRecords,
        StatsTightBounds,
        StatsMinValues,
        StatsMaxValues,
        StatsNullCount,
        // The first leaf of a top-level column outside the known actions
        Unknown
    }

    /// <summary>
    /// One leaf of a root the scan admits and the reader decodes.
    /// </summary>
    internal sealed record ProjectedLeaf(int Index, string LogicalPath, ProjectedRole Role, Leaf Field, ParquetPhysicalType Type, int MaxDefinition, int MaxRepetition, bool Required)
    {
        // The definition level each path element is defined at, top-level column first; a map or list adds its repeated wrapper before the leaf
        public required int[] Definitions { get; init; }

        // The definition level at which a level is an entry of the innermost collection, or -1 when the leaf does not count entries (a map value)
        public int EntryDefinition { get; init; } = -1;

        // The definition level of the innermost repeated element, -1 for a leaf outside collections
        public int RepeatedDefinition { get; init; } = -1;
    }

    /// <summary>
    /// The leaves of a root that are read, map and list wrappers named logically.
    /// </summary>
    internal sealed class CheckpointProjection
    {
        private static readonly HashSet<Leaf> s_mapValues = new HashSet<Leaf>();

        // The path of each field we read: a scalar, or the map or list that holds it
        private static readonly HashSet<string> s_fieldRoots = new HashSet<string>(StringComparer.Ordinal);

        // Structs whose unknown members are ignored: the actions, the deletion vectors and the format
        private static readonly HashSet<string> s_structs = new HashSet<string>(StringComparer.Ordinal);

        private static readonly Dictionary<string, Leaf> s_fields = BuildFields();

        private CheckpointProjection(List<ProjectedLeaf> leaves, int leafCount)
        {
            Leaves = leaves;
            LeafCount = leafCount;
        }

        public IReadOnlyList<ProjectedLeaf> Leaves { get; }

        // All leaves of the file, projected or not
        public int LeafCount { get; }

        // The same file with fewer or other leaves: a pre-read, a classification, or a doctored test fixture
        internal CheckpointProjection WithLeaves(List<ProjectedLeaf> leaves) => new CheckpointProjection(leaves, LeafCount);

        public bool[] Mask()
        {
            var mask = new bool[LeafCount];
            foreach (var leaf in Leaves)
            {
                mask[leaf.Index] = true;
            }
            return mask;
        }

        private static Dictionary<string, Leaf> BuildFields()
        {
            var fields = new Dictionary<string, Leaf>(StringComparer.Ordinal);
            void Struct(string prefix, Apache.Arrow.Types.StructType type, ref int leaf)
            {
                s_structs.Add(prefix);
                foreach (var field in type.Fields)
                {
                    var path = prefix + "." + field.Name;
                    switch (field.DataType)
                    {
                        case Apache.Arrow.Types.MapType:
                            fields[path + ".key"] = (Leaf)leaf++;
                            s_mapValues.Add((Leaf)leaf);
                            fields[path + ".value"] = (Leaf)leaf++;
                            s_fieldRoots.Add(path);
                            break;
                        case Apache.Arrow.Types.ListType:
                            fields[path + ".element"] = (Leaf)leaf++;
                            s_fieldRoots.Add(path);
                            break;
                        case Apache.Arrow.Types.StructType nested:
                            Struct(path, nested, ref leaf);
                            break;
                        default:
                            fields[path] = (Leaf)leaf++;
                            s_fieldRoots.Add(path);
                            break;
                    }
                }
            }
            var index = 0;
            foreach (var field in CheckpointSchema.Build(null).FieldsList)
            {
                Struct(field.Name, (Apache.Arrow.Types.StructType)field.DataType, ref index);
            }
            if (index != CheckpointSchema.LeafCount)
            {
                throw new InvalidOperationException($"The schema has {index} leaves");
            }
            // The sidecar action is read for its path alone, which decides that a root is unsupported
            s_structs.Add("sidecar");
            s_fieldRoots.Add("sidecar.path");
            return fields;
        }

        /// <summary>
        /// The projection of a file schema, given as Parquet's depth-first element list.
        /// </summary>
        public static CheckpointProjection Of(IReadOnlyList<FooterSchemaElement> schema)
        {
            if (schema.Count == 0 || schema[0].Type != null)
            {
                throw new ThriftFormatException("The schema has no root group");
            }
            var leaves = new List<WalkedLeaf>();
            var position = 1;
            for (int c = 0; c < schema[0].Children; c++)
            {
                Walk(schema, ref position, ImmutableArray<string>.Empty, 0, 0, -1, ImmutableArray<int>.Empty, -1, leaves, logicalName: null);
            }
            if (position != schema.Count)
            {
                throw new ThriftFormatException("The schema's child counts do not cover its elements");
            }

            var projected = new List<ProjectedLeaf>();
            var firstStats = new HashSet<ProjectedRole>();
            var unknownColumns = new HashSet<string>(StringComparer.Ordinal);
            var mapped = new bool[CheckpointSchema.LeafCount];
            for (int i = 0; i < leaves.Count; i++)
            {
                var (segments, element, definition, repetition, required, definitions, entryDefinition, repeatedAt) = leaves[i];
                // Paths are matched by segment, so a name holding a dot never stands in for a nested field
                var top = segments[0];
                ProjectedRole? role = null;
                var field = default(Leaf);
                if (Key(segments) is { } key && s_fields.TryGetValue(key, out var known))
                {
                    if (mapped[(int)known])
                    {
                        throw new ThriftFormatException($"Field {key} appears twice");
                    }
                    if (repetition != (CheckpointSchema.RepeatedOf(known) >= 0 ? 1 : 0))
                    {
                        throw new ThriftFormatException($"Field {key} has repetition {repetition}");
                    }
                    mapped[(int)known] = true;
                    role = ProjectedRole.Field;
                    field = known;
                }
                else if (Is(segments, "sidecar", "path") && repeatedAt < 0)
                {
                    role = ProjectedRole.SidecarPath;
                }
                else if (InAnotherShape(segments, repeatedAt))
                {
                    throw new ThriftFormatException($"{Display(segments)} is a known field in another shape");
                }
                else if (Is(segments, "add", "stats_parsed", "numRecords"))
                {
                    role = ProjectedRole.StatsNumRecords;
                }
                else if (Is(segments, "add", "stats_parsed", "tightBounds"))
                {
                    role = ProjectedRole.StatsTightBounds;
                }
                else if (segments.Length > 3 && segments[0] == "add" && segments[1] == "stats_parsed")
                {
                    role = segments[2] switch
                    {
                        "minValues" => ProjectedRole.StatsMinValues,
                        "maxValues" => ProjectedRole.StatsMaxValues,
                        "nullCount" => ProjectedRole.StatsNullCount,
                        _ => null
                    };
                }
                else if (!CheckpointCapability.KnownColumns.Contains(top) && unknownColumns.Add(top))
                {
                    role = ProjectedRole.Unknown;
                }
                if (role is ProjectedRole.StatsMinValues or ProjectedRole.StatsMaxValues or ProjectedRole.StatsNullCount && !firstStats.Add(role.Value))
                {
                    role = null;
                }
                if (role != null)
                {
                    // The entry cap counts a map's entries on its key, so the value leaf is not counted again
                    var counts = repetition > 0 && !(role == ProjectedRole.Field && s_mapValues.Contains(field));
                    projected.Add(new ProjectedLeaf(i, Display(segments), role.Value, field, element.Type!.Value, definition, repetition, required)
                    {
                        Definitions = definitions,
                        EntryDefinition = counts ? entryDefinition : -1,
                        RepeatedDefinition = repetition > 0 ? entryDefinition : -1
                    });
                }
            }
            return new CheckpointProjection(projected, leaves.Count);
        }

        // RepeatedAt is the depth of the first repeated or collection element on the path, -1 for none
        private readonly record struct WalkedLeaf(ImmutableArray<string> Segments, FooterSchemaElement Element, int Definition, int Repetition, bool Required, int[] Definitions, int EntryDefinition, int RepeatedAt);

        private static readonly int s_longestField = s_fields.Keys.Max(x => x.Length);

        // The dotted key of a path whose segments hold no dots and which is no longer than our longest field, else null
        private static string? Key(ImmutableArray<string> segments)
        {
            var length = segments.Length - 1;
            foreach (var segment in segments)
            {
                length += segment.Length;
                if (length > s_longestField || segment.Contains('.'))
                {
                    return null;
                }
            }
            return string.Join('.', segments);
        }

        // A leaf that is no field we read, at or below a field's path or in place of a struct; an unknown member of a struct is ignored
        private static bool InAnotherShape(ImmutableArray<string> segments, int repeatedAt)
        {
            var length = 0;
            for (int depth = 1; depth <= segments.Length; depth++)
            {
                length += segments[depth - 1].Length + (depth > 1 ? 1 : 0);
                if (length > s_longestField || segments[depth - 1].Contains('.'))
                {
                    return false;
                }
                var prefix = string.Join('.', segments.Take(depth));
                // A struct held in a list, a map or a repeated group is that struct in another shape too
                if (s_fieldRoots.Contains(prefix) || (s_structs.Contains(prefix) && (depth == segments.Length || (repeatedAt >= 0 && repeatedAt < depth))))
                {
                    return true;
                }
            }
            return false;
        }

        private static bool Is(ImmutableArray<string> segments, params string[] expected)
        {
            if (segments.Length != expected.Length)
            {
                return false;
            }
            for (int i = 0; i < expected.Length; i++)
            {
                if (segments[i] != expected[i])
                {
                    return false;
                }
            }
            return true;
        }

        // A path for messages, cut short so a crafted schema cannot make it large
        private static string Display(ImmutableArray<string> segments)
        {
            var path = string.Join('.', segments.Select(x => x.Length > 64 ? x.Substring(0, 64) + "..." : x));
            return path.Length > 256 ? path.Substring(0, 256) + "..." : path;
        }

        private static void Walk(IReadOnlyList<FooterSchemaElement> schema, ref int position, ImmutableArray<string> prefix, int definition, int repetition, int entry, ImmutableArray<int> definitions,
            int repeatedAt, List<WalkedLeaf> leaves, string? logicalName)
        {
            if (position >= schema.Count)
            {
                throw new ThriftFormatException("The schema's child counts run past its elements");
            }
            if (prefix.Length >= MaxDepth)
            {
                throw new ThriftFormatException($"The schema nests deeper than {MaxDepth}");
            }
            var element = schema[position++];
            var path = prefix.Add(logicalName ?? element.Name);
            if (element.Repetition == ParquetRepetition.Optional)
            {
                definition++;
            }
            else if (element.Repetition == ParquetRepetition.Repeated)
            {
                definition++;
                repetition++;
                entry = definition;
                repeatedAt = repeatedAt < 0 ? path.Length - 1 : repeatedAt;
            }
            definitions = definitions.Add(definition);
            if (element.Type != null)
            {
                if (element.Children != 0)
                {
                    throw new ThriftFormatException($"Leaf {Display(path)} has children");
                }
                leaves.Add(new WalkedLeaf(path, element, definition, repetition, element.Repetition == ParquetRepetition.Required, definitions.ToArray(), entry, repeatedAt));
                return;
            }
            if (element.IsMapOrList && element.Children == 1 && position < schema.Count && schema[position].Repetition == ParquetRepetition.Repeated)
            {
                // The repeated wrapper is skipped by name: two children are a key and a value, one child or a repeated leaf is the element
                var wrapper = schema[position];
                repeatedAt = repeatedAt < 0 ? path.Length - 1 : repeatedAt;
                if (wrapper.Type != null)
                {
                    // A repeated leaf is its own wrapper and element
                    Walk(schema, ref position, path, definition, repetition, entry, definitions.Add(definition + 1), repeatedAt, leaves, "element");
                    return;
                }
                position++;
                var inner = definition + 1;
                var innerRepetition = repetition + 1;
                var wrapped = definitions.Add(inner);
                entry = inner;
                if (wrapper.Children == 2)
                {
                    Walk(schema, ref position, path, inner, innerRepetition, entry, wrapped, repeatedAt, leaves, "key");
                    Walk(schema, ref position, path, inner, innerRepetition, entry, wrapped, repeatedAt, leaves, "value");
                }
                else if (wrapper.Children == 1)
                {
                    Walk(schema, ref position, path, inner, innerRepetition, entry, wrapped, repeatedAt, leaves, "element");
                }
                else
                {
                    var named = path.Add(wrapper.Name);
                    for (int c = 0; c < wrapper.Children; c++)
                    {
                        Walk(schema, ref position, named, inner, innerRepetition, entry, wrapped, repeatedAt, leaves, null);
                    }
                }
                return;
            }
            for (int c = 0; c < element.Children; c++)
            {
                Walk(schema, ref position, path, definition, repetition, entry, definitions, repeatedAt, leaves, null);
            }
        }

        // Deeper than any checkpoint schema, shallow enough for the recursive walk
        public const int MaxDepth = 64;
    }
}
