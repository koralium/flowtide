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

using FlowtideDotNet.DependencyInjection;

namespace FlowtideDotNet.Storage.SqlServer
{
    public static class SqlServerStorageExtensions
    {
        /// <summary>
        /// Adds SQL Server storage to the Flowtide storage builder using the specified connection string.
        /// </summary>
        /// <param name="storageBuilder">The Flowtide storage builder.</param>
        /// <param name="connectionString">The connection string for the SQL Server database.</param>
        /// <returns>The updated Flowtide storage builder.</returns>
        public static IFlowtideStorageBuilder AddSqlServerStorage
            (this IFlowtideStorageBuilder storageBuilder, string connectionString)
        {
            storageBuilder.SetPersistentStorage(new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionStringFunc = () => connectionString,
            }));

            return storageBuilder;
        }

        public static IFlowtideStorageBuilder AddSqlServerStorage(this IFlowtideStorageBuilder storageBuilder, Func<string> connectionStringFunc)
        {
            storageBuilder.SetPersistentStorage(new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionStringFunc = connectionStringFunc,
            }));

            return storageBuilder;
        }

        /// <summary>
        /// Adds SQL Server storage to the Flowtide storage builder using the specified settings.
        /// </summary>
        /// <param name="storageBuilder">The Flowtide storage builder.</param>
        /// <param name="settings">The settings for the SQL Server persistent storage.</param>
        /// <returns>The updated Flowtide storage builder.</returns>
        public static IFlowtideStorageBuilder AddSqlServerStorage
            (this IFlowtideStorageBuilder storageBuilder, SqlServerPersistentStorageSettings settings)
        {
            storageBuilder.SetPersistentStorage(new SqlServerPersistentStorage(settings));

            return storageBuilder;
        }
    }
}
