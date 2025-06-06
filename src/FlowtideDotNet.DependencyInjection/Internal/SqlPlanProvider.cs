﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Core;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.DependencyInjection.Internal
{
    internal class SqlPlanProvider : IFlowtidePlanProvider
    {
        private readonly string sql;
        private readonly IConnectorManager connectorManager;

        public SqlPlanProvider(string sql, IConnectorManager connectorManager)
        {
            this.sql = sql;
            this.connectorManager = connectorManager;
        }

        public Plan GetPlan()
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            foreach (var tableProvider in connectorManager.GetTableProviders())
            {
                sqlPlanBuilder.AddTableProvider(tableProvider);
            }
            sqlPlanBuilder.Sql(sql);
            return sqlPlanBuilder.GetPlan();
        }
    }
}
