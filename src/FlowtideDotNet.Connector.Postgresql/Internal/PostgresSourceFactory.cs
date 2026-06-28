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

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Connector.PostgreSQL;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using Npgsql;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    public class PostgresSourceFactory : AbstractConnectorSourceFactory, IConnectorTableProviderFactory
    {
        private readonly PostgresSourceOptions _options;
        private readonly PostgresTableProvider _tableProvider;
        private readonly PostgresReplicationCoordinator _coordinator;
        private readonly object _validateLock = new();
        private volatile bool _validated;

        public PostgresSourceFactory(PostgresSourceOptions options)
        {
            _options = options;
            _tableProvider = new PostgresTableProvider(options.ConnectionStringFunc);
            _coordinator = new PostgresReplicationCoordinator(options);
        }

        public override bool CanHandle(ReadRelation readRelation)
        {
            var tableName = _options.TableNameTransform?.Invoke(readRelation) ?? readRelation.NamedTable.Names;
            return _tableProvider.TryGetTableInformation(tableName, out _);
        }

        public ITableProvider Create() => _tableProvider;

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            var tableName = _options.TableNameTransform?.Invoke(readRelation) ?? readRelation.NamedTable.Names;
            var (schema, table) = PostgresUtils.ResolveSchemaAndTable(tableName);

            using var connection = new NpgsqlConnection(_options.ConnectionStringFunc());
            connection.Open();
            var primaryKeys = PostgresUtils.GetPrimaryKeys(connection, schema, table, default).GetAwaiter().GetResult();

            foreach (var pk in primaryKeys)
            {
                var index = readRelation.BaseSchema.Names.FindIndex(n => n.Equals(pk, StringComparison.OrdinalIgnoreCase));
                if (index == -1)
                {
                    readRelation.BaseSchema.Names.Add(pk);
                    readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                }
            }

            return readRelation;
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            ValidatePrerequisites();

            var tableName = _options.TableNameTransform?.Invoke(readRelation) ?? readRelation.NamedTable.Names;
            var (schema, table) = PostgresUtils.ResolveSchemaAndTable(tableName);

            // Fail fast at startup: the source needs a primary key (it keys its reconciling state on it), and the table
            // must have a replica identity that carries the key in UPDATE/DELETE WAL records (default = PK, or FULL).
            using (var connection = new NpgsqlConnection(_options.ConnectionStringFunc()))
            {
                connection.Open();
                var primaryKeys = PostgresUtils.GetPrimaryKeys(connection, schema, table, default).GetAwaiter().GetResult();
                if (primaryKeys.Count == 0)
                {
                    throw new InvalidOperationException(
                        $"Table {schema}.{table} has no primary key. A primary key is required for the PostgreSQL source.");
                }
                var replicaIdentity = PostgresUtils.GetReplicaIdentity(connection, schema, table, default).GetAwaiter().GetResult();
                if (replicaIdentity == 'n')
                {
                    throw new InvalidOperationException(
                        $"Table {schema}.{table} has REPLICA IDENTITY NOTHING, so UPDATE/DELETE changes carry no key and cannot be applied. Set REPLICA IDENTITY to DEFAULT (the primary key) or FULL.");
                }
            }

            // Record table membership at plan time so the shared reader can build one publication for all tables.
            _coordinator.RegisterTable(schema, table);

            Func<PostgresChangeSourceContext, IPostgresChangeSource> changeSourceFactory = _options.ReplicationMode switch
            {
                PostgresReplicationMode.PerTable => context => new PerTableChangeSource(
                    _options,
                    context.StreamName,
                    context.OperatorName,
                    context.Schema,
                    context.Table,
                    context.SchemaNames,
                    context.KeySchemaIndices),
                PostgresReplicationMode.Shared => context => _coordinator.CreateChangeSource(context),
                _ => throw new NotSupportedException($"Unknown replication mode {_options.ReplicationMode}.")
            };

            return new ColumnPostgresDeltaSource(_options, readRelation, functionsRegister, dataflowBlockOptions, changeSourceFactory);
        }

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
        {
            var tableName = _options.TableNameTransform?.Invoke(readRelation) ?? readRelation.NamedTable.Names;
            return new TableLineageMetadata(
                "postgres",
                readRelation.NamedTable.DotSeperated,
                includeSchema && _tableProvider.TryGetTableInformation(tableName, out var metadata)
                    ? metadata.Schema
                    : default);
        }

        private void ValidatePrerequisites()
        {
            if (_validated)
            {
                return;
            }
            lock (_validateLock)
            {
                if (_validated)
                {
                    return;
                }

                using var connection = new NpgsqlConnection(_options.ConnectionStringFunc());
                connection.Open();

                var walLevel = PostgresUtils.GetWalLevel(connection, default).GetAwaiter().GetResult();
                if (!string.Equals(walLevel, "logical", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        $"PostgreSQL logical replication requires 'wal_level = logical' but the server reports '{walLevel}'.");
                }

                var hasPermission = PostgresUtils.HasReplicationPermission(connection, default).GetAwaiter().GetResult();
                if (!hasPermission)
                {
                    throw new InvalidOperationException(
                        "The connecting role must have the REPLICATION attribute (or be a superuser) to use the PostgreSQL source.");
                }

                _validated = true;
            }
        }
    }
}
