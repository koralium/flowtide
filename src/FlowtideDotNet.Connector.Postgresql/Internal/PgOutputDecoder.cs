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

using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    /// <summary>
    /// Maps the columns of a published relation onto the read relation's schema column order.
    /// </summary>
    internal sealed class RelationColumnMap
    {
        /// <summary>relation column index -> schema column index, or -1 when the column is not part of the schema.</summary>
        public required int[] RelationToSchema { get; init; }

        public required int SchemaWidth { get; init; }

        /// <summary>Schema column indices that make up the table's key (used to detect key-changing updates).</summary>
        public required int[] KeySchemaIndices { get; init; }

        public static RelationColumnMap Build(RelationMessage relation, IReadOnlyList<string> schemaNames, IReadOnlyList<int> keySchemaIndices)
        {
            var nameToSchema = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < schemaNames.Count; i++)
            {
                nameToSchema[schemaNames[i]] = i;
            }

            var relationToSchema = new int[relation.Columns.Count];
            for (int i = 0; i < relation.Columns.Count; i++)
            {
                relationToSchema[i] = nameToSchema.TryGetValue(relation.Columns[i].ColumnName, out var idx) ? idx : -1;
            }

            return new RelationColumnMap
            {
                RelationToSchema = relationToSchema,
                SchemaWidth = schemaNames.Count,
                KeySchemaIndices = keySchemaIndices.ToArray()
            };
        }
    }

    internal static class PgOutputDecoder
    {
        public static async Task<PostgresChange> DecodeInsertAsync(InsertMessage message, RelationColumnMap map, CancellationToken ct)
        {
            var values = new object?[map.SchemaWidth];
            await ReadTupleAsync(message.NewRow, map, values, ct);
            return new PostgresChange
            {
                Kind = PostgresChangeKind.Insert,
                Values = values,
                Lsn = (ulong)message.WalEnd
            };
        }

        public static async Task<PostgresChange> DecodeUpdateAsync(UpdateMessage message, RelationColumnMap map, CancellationToken ct)
        {
            object?[]? oldImage = null;

            // The old tuple (when present) is sent before the new tuple on the wire and must be read first.
            switch (message)
            {
                case IndexUpdateMessage indexUpdate:
                    oldImage = new object?[map.SchemaWidth];
                    await ReadTupleAsync(indexUpdate.Key, map, oldImage, ct);
                    break;
                case FullUpdateMessage fullUpdate:
                    oldImage = new object?[map.SchemaWidth];
                    await ReadTupleAsync(fullUpdate.OldRow, map, oldImage, ct);
                    break;
            }

            var values = new object?[map.SchemaWidth];
            await ReadTupleAsync(message.NewRow, map, values, ct);

            object?[]? oldKey = null;
            if (oldImage != null && KeyDiffers(oldImage, values, map.KeySchemaIndices))
            {
                oldKey = oldImage;
            }

            return new PostgresChange
            {
                Kind = PostgresChangeKind.Update,
                Values = values,
                OldKeyValues = oldKey,
                Lsn = (ulong)message.WalEnd
            };
        }

        public static async Task<PostgresChange> DecodeDeleteAsync(DeleteMessage message, RelationColumnMap map, CancellationToken ct)
        {
            var key = new object?[map.SchemaWidth];
            switch (message)
            {
                case KeyDeleteMessage keyDelete:
                    await ReadTupleAsync(keyDelete.Key, map, key, ct);
                    break;
                case FullDeleteMessage fullDelete:
                    await ReadTupleAsync(fullDelete.OldRow, map, key, ct);
                    break;
            }

            return new PostgresChange
            {
                Kind = PostgresChangeKind.Delete,
                Values = key,
                Lsn = (ulong)message.WalEnd
            };
        }

        private static async Task ReadTupleAsync(ReplicationTuple tuple, RelationColumnMap map, object?[] target, CancellationToken ct)
        {
            int position = 0;
            await foreach (var value in tuple.WithCancellation(ct))
            {
                int schemaIndex = position < map.RelationToSchema.Length ? map.RelationToSchema[position] : -1;
                var read = await ReadValueAsync(value, ct);
                if (schemaIndex >= 0)
                {
                    target[schemaIndex] = read;
                }
                position++;
            }
        }

        private static async ValueTask<object?> ReadValueAsync(ReplicationValue value, CancellationToken ct)
        {
            switch (value.Kind)
            {
                case TupleDataKind.Null:
                    return null;
                case TupleDataKind.UnchangedToastedValue:
                    return PostgresUtils.UnchangedToast;
                default:
                    return await value.Get(ct);
            }
        }

        private static bool KeyDiffers(object?[] oldImage, object?[] newImage, int[] keySchemaIndices)
        {
            foreach (var idx in keySchemaIndices)
            {
                var oldValue = oldImage[idx];
                var newValue = newImage[idx];
                if (ReferenceEquals(oldValue, PostgresUtils.UnchangedToast) || ReferenceEquals(newValue, PostgresUtils.UnchangedToast))
                {
                    continue;
                }
                if (!Equals(oldValue, newValue))
                {
                    return true;
                }
            }
            return false;
        }
    }
}
