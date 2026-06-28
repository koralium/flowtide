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

    /// <summary>
    /// A change decoded from the WAL into <b>relation</b>-column order (one entry per published column). The wire tuple
    /// can only be read once, so the streaming loop reads it into this form and then projects it onto each interested
    /// source's schema via <see cref="PgOutputDecoder.Project"/> - which is what lets several operators (e.g. both sides
    /// of a self-join) consume the same table's changes.
    /// </summary>
    internal sealed class RawPgChange
    {
        public required PostgresChangeKind Kind { get; init; }

        /// <summary>New tuple (insert/update) or the deleted row's key, in relation column order.</summary>
        public required object?[] NewRelationValues { get; init; }

        /// <summary>Old tuple image for an update (relation order), when the replica identity provides one. Null otherwise.</summary>
        public object?[]? OldRelationValues { get; init; }

        public required ulong Lsn { get; init; }
    }

    internal static class PgOutputDecoder
    {
        public static async Task<RawPgChange> ReadInsertAsync(InsertMessage message, CancellationToken ct)
        {
            var values = await ReadTupleAsync(message.NewRow, message.Relation.Columns.Count, ct);
            return new RawPgChange
            {
                Kind = PostgresChangeKind.Insert,
                NewRelationValues = values,
                Lsn = (ulong)message.WalEnd
            };
        }

        public static async Task<RawPgChange> ReadUpdateAsync(UpdateMessage message, CancellationToken ct)
        {
            object?[]? oldImage = null;

            // The old tuple (when present) is sent before the new tuple on the wire and must be read first.
            switch (message)
            {
                case IndexUpdateMessage indexUpdate:
                    oldImage = await ReadTupleAsync(indexUpdate.Key, message.Relation.Columns.Count, ct);
                    break;
                case FullUpdateMessage fullUpdate:
                    oldImage = await ReadTupleAsync(fullUpdate.OldRow, message.Relation.Columns.Count, ct);
                    break;
            }

            var values = await ReadTupleAsync(message.NewRow, message.Relation.Columns.Count, ct);

            return new RawPgChange
            {
                Kind = PostgresChangeKind.Update,
                NewRelationValues = values,
                OldRelationValues = oldImage,
                Lsn = (ulong)message.WalEnd
            };
        }

        public static async Task<RawPgChange> ReadDeleteAsync(DeleteMessage message, CancellationToken ct)
        {
            object?[] key = message switch
            {
                KeyDeleteMessage keyDelete => await ReadTupleAsync(keyDelete.Key, message.Relation.Columns.Count, ct),
                FullDeleteMessage fullDelete => await ReadTupleAsync(fullDelete.OldRow, message.Relation.Columns.Count, ct),
                _ => new object?[message.Relation.Columns.Count]
            };

            return new RawPgChange
            {
                Kind = PostgresChangeKind.Delete,
                NewRelationValues = key,
                Lsn = (ulong)message.WalEnd
            };
        }

        /// <summary>
        /// Projects a relation-order raw change onto one source's schema column order, producing the change that source's
        /// operator consumes. Safe to call several times for the same <paramref name="raw"/> (once per interested source).
        /// </summary>
        public static PostgresChange Project(RawPgChange raw, RelationColumnMap map)
        {
            var values = ProjectValues(raw.NewRelationValues, map);

            object?[]? oldKey = null;
            if (raw.Kind == PostgresChangeKind.Update && raw.OldRelationValues != null)
            {
                var oldImage = ProjectValues(raw.OldRelationValues, map);
                if (KeyDiffers(oldImage, values, map.KeySchemaIndices))
                {
                    oldKey = oldImage;
                }
            }

            return new PostgresChange
            {
                Kind = raw.Kind,
                Values = values,
                OldKeyValues = oldKey,
                Lsn = raw.Lsn
            };
        }

        private static object?[] ProjectValues(object?[] relationValues, RelationColumnMap map)
        {
            var schemaValues = new object?[map.SchemaWidth];
            var relationToSchema = map.RelationToSchema;
            int count = Math.Min(relationValues.Length, relationToSchema.Length);
            for (int i = 0; i < count; i++)
            {
                int schemaIndex = relationToSchema[i];
                if (schemaIndex >= 0)
                {
                    schemaValues[schemaIndex] = relationValues[i];
                }
            }
            return schemaValues;
        }

        private static async Task<object?[]> ReadTupleAsync(ReplicationTuple tuple, int relationColumnCount, CancellationToken ct)
        {
            var values = new object?[relationColumnCount];
            int position = 0;
            await foreach (var value in tuple.WithCancellation(ct))
            {
                var read = await ReadValueAsync(value, ct);
                if (position < values.Length)
                {
                    values[position] = read;
                }
                position++;
            }
            return values;
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
