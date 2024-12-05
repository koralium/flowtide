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

using FlowtideDotNet.Connector.SqlServer;
using FlowtideDotNet.Connector.SqlServer.SqlServer;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core
{
    public static class SqlServerConnectorManagerExtensions
    {
        public static IConnectorManager AddSqlServerSource(
            this IConnectorManager connectorManager, 
            Func<string> connectionStringFunc,
            Func<ReadRelation, string>? tableNameTransform = null,
            bool useDatabaseDefinedInConnectionStringOnly = false)
        {
            connectorManager.AddSource(new SqlServerSourceFactory(connectionStringFunc, tableNameTransform, useDatabaseDefinedInConnectionStringOnly));
            return connectorManager;
        }

        public static IConnectorManager AddSqlServerSink(this IConnectorManager connectorManager, Func<string> connectionStringFunc)
        {
            connectorManager.AddSink(new SqlServerSinkFactory(new SqlServerSinkOptions()
            {
                ConnectionStringFunc = connectionStringFunc
            }));
            return connectorManager;
        }

        public static IConnectorManager AddSqlServerSink(this IConnectorManager connectorManager, SqlServerSinkOptions options)
        {
            connectorManager.AddSink(new SqlServerSinkFactory(options));
            return connectorManager;
        }

        /// <summary>
        /// Add both source and sink for a sql server connection string under a catalog.
        /// 
        /// Reference the tables with {catalogName}.{tableName}
        /// </summary>
        /// <param name="connectorManager"></param>
        /// <param name="catalogName"></param>
        /// <param name="connectionStringFunc"></param>
        /// <returns></returns>
        public static IConnectorManager AddSqlServerAsCatalog(this IConnectorManager connectorManager, string catalogName, Func<string> connectionStringFunc)
        {
            connectorManager.AddCatalog(catalogName, opt =>
            {
                opt.AddSqlServerSource(connectionStringFunc);
                opt.AddSqlServerSink(connectionStringFunc);
            });
            return connectorManager;
        }
    }
}
