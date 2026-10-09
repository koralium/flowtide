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

using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Substrait.Type;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal static class LineageMerge
    {
        public static ColumnLineage Merge(IReadOnlyList<ColumnLineage> lineages)
        {
            if (lineages.Count == 0)
            {
                throw new ArgumentException("At least one column lineage is required.", nameof(lineages));
            }
            if (lineages.Count == 1)
            {
                return lineages[0];
            }

            // Insert only, keeps first seen column order.
            var fieldInputs = new Dictionary<string, List<IReadOnlyList<LineageInputField>>>(StringComparer.Ordinal);
            foreach (var lineage in lineages)
            {
                foreach (var field in lineage.Fields)
                {
                    if (!fieldInputs.TryGetValue(field.Key, out var inputLists))
                    {
                        inputLists = new List<IReadOnlyList<LineageInputField>>();
                        fieldInputs.Add(field.Key, inputLists);
                    }
                    inputLists.Add(field.Value.InputFields);
                }
            }

            var fields = new Dictionary<string, ColumnLineageField>(StringComparer.Ordinal);
            foreach (var kv in fieldInputs)
            {
                fields.Add(kv.Key, new ColumnLineageField(MergeInputFieldLists(kv.Value)));
            }

            var dataset = MergeInputFieldLists(lineages.Select(x => x.Dataset));
            return new ColumnLineage(fields, dataset);
        }

        public static IReadOnlyList<LineageInputField> MergeInputFields(IReadOnlyList<LineageInputField> first, IReadOnlyList<LineageInputField> second)
        {
            return MergeInputFieldLists([first, second]);
        }

        public static IReadOnlyList<LineageTransformation> MergeTransformations(IReadOnlyList<LineageTransformation> first, IReadOnlyList<LineageTransformation> second)
        {
            var result = new List<LineageTransformation>(first.Count + second.Count);
            foreach (var transformation in first.Concat(second))
            {
                if (!result.Contains(transformation))
                {
                    result.Add(transformation);
                }
            }

            // Same rule as AppendTransformation, identity loses to direct changes.
            if (result.Any(IsDirectChange))
            {
                result.RemoveAll(t => t.Type == LineageTransformationType.Direct && t.SubType == LineageTransformationSubtype.Identity);
            }
            return result;
        }

        public static IReadOnlyList<LineageColumn> MergeColumns(IEnumerable<IReadOnlyList<LineageColumn>> columnSets)
        {
            var result = new List<LineageColumn>();
            var indexByName = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var columns in columnSets)
            {
                foreach (var column in columns)
                {
                    if (!indexByName.TryGetValue(column.Name, out var index))
                    {
                        indexByName.Add(column.Name, result.Count);
                        result.Add(column);
                    }
                    else if (IsUnknown(result[index].Type) && !IsUnknown(column.Type))
                    {
                        result[index] = column;
                    }
                }
            }
            return result;
        }

        // Same rule as the DataHub column set.
        private static bool IsUnknown(SubstraitBaseType type)
        {
            return type is AnyType || type is NullType;
        }

        [return: NotNullIfNotNull(nameof(schema))]
        public static IReadOnlyList<LineageColumn>? ToColumns(NamedStruct? schema)
        {
            if (schema == null)
            {
                return null;
            }

            var types = schema.Struct?.Types;
            var columns = new List<LineageColumn>(schema.Names.Count);
            for (int i = 0; i < schema.Names.Count; i++)
            {
                var type = types != null && i < types.Count ? types[i] : null;
                columns.Add(new LineageColumn(schema.Names[i], type ?? AnyType.Instance));
            }
            return columns;
        }

        private static IReadOnlyList<LineageInputField> MergeInputFieldLists(IEnumerable<IReadOnlyList<LineageInputField>> lists)
        {
            var order = new List<(string, string, string)>();
            var merged = new Dictionary<(string, string, string), LineageInputField>();
            foreach (var list in lists)
            {
                foreach (var field in list)
                {
                    // Tuple identity, dots in names never collide.
                    var key = (field.Namespace, field.TableName, field.Field);
                    if (merged.TryGetValue(key, out var existing))
                    {
                        merged[key] = new LineageInputField(
                            existing.Namespace,
                            existing.TableName,
                            existing.Field,
                            MergeTransformations(existing.Transformations, field.Transformations));
                    }
                    else
                    {
                        merged.Add(key, field);
                        order.Add(key);
                    }
                }
            }

            var result = new List<LineageInputField>(order.Count);
            foreach (var key in order)
            {
                result.Add(merged[key]);
            }
            return result;
        }

        private static bool IsDirectChange(LineageTransformation transformation)
        {
            return transformation.Type == LineageTransformationType.Direct &&
                (transformation.SubType == LineageTransformationSubtype.Transformation ||
                transformation.SubType == LineageTransformationSubtype.Aggregation);
        }
    }
}
