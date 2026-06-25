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

using FlowtideDotNet.Connector.PostgreSQL;
using FlowtideDotNet.Connector.PostgreSQL.Internal;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core
{
    public static class PostgreSqlConnectorManagerExtensions
    {
        /// <summary>
        /// Adds a PostgreSQL logical replication source. Tables are referenced as {schema}.{table}.
        /// </summary>
        public static IConnectorManager AddPostgresSource(
            this IConnectorManager connectorManager,
            Func<string> connectionStringFunc,
            PostgresReplicationMode replicationMode = PostgresReplicationMode.Shared,
            Func<ReadRelation, IReadOnlyList<string>>? tableNameTransform = null)
        {
            connectorManager.AddSource(new PostgresSourceFactory(new PostgresSourceOptions
            {
                ConnectionStringFunc = connectionStringFunc,
                ReplicationMode = replicationMode,
                TableNameTransform = tableNameTransform
            }));
            return connectorManager;
        }

        /// <summary>
        /// Adds a PostgreSQL logical replication source using the provided options.
        /// </summary>
        public static IConnectorManager AddPostgresSource(
            this IConnectorManager connectorManager,
            PostgresSourceOptions options)
        {
            connectorManager.AddSource(new PostgresSourceFactory(options));
            return connectorManager;
        }

        /// <summary>
        /// Adds a PostgreSQL source under a catalog. Reference tables with {catalogName}.{schema}.{table}.
        /// </summary>
        public static IConnectorManager AddPostgresAsCatalog(
            this IConnectorManager connectorManager,
            string catalogName,
            Func<string> connectionStringFunc,
            PostgresReplicationMode replicationMode = PostgresReplicationMode.Shared)
        {
            connectorManager.AddCatalog(catalogName, opt =>
            {
                opt.AddPostgresSource(connectionStringFunc, replicationMode);
            });
            return connectorManager;
        }
    }
}
