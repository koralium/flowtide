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

using FlowtideDotNet.Core;
using FlowtideDotNet.DependencyInjection.Internal;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.DependencyInjection;

namespace FlowtideDotNet.DependencyInjection
{
    public static class FlowtideDIBuilderExtensions
    {
        public static IFlowtideDIBuilder AddSqlFileAsPlan(this IFlowtideDIBuilder builder, string filePath)
        {
            var sqlText = File.ReadAllText(filePath);
            return builder.AddSqlTextAsPlan(sqlText);
        }

        public static IFlowtideDIBuilder AddSqlTextAsPlan(this IFlowtideDIBuilder builder, string sqlText)
        {
            builder.Services.AddKeyedSingleton<IFlowtidePlanProvider>(builder.StreamName, (provider, key) =>
            {
                var connectorManager = provider.GetRequiredKeyedService<IConnectorManager>(builder.StreamName);
                return new SqlPlanProvider(sqlText, connectorManager);
            });
            return builder;
        }

        public static IFlowtideDIBuilder AddPlan(this IFlowtideDIBuilder builder, Plan plan)
        {
            builder.Services.AddKeyedSingleton<IFlowtidePlanProvider>(builder.StreamName, (provider, key) =>
            {
                return new PlanProvider(plan);
            });
            return builder;
        }

        public static IFlowtideDIBuilder AddSqlPlan(this IFlowtideDIBuilder builder, Action<SqlPlanBuilder> configure)
        {
            builder.Services.AddKeyedSingleton<IFlowtidePlanProvider>(builder.StreamName, (provider, key) =>
            {
                var connectorManager = provider.GetRequiredKeyedService<IConnectorManager>(builder.StreamName);
                var sqlPlanBuilder = new SqlPlanBuilder();
                foreach (var tableProvider in connectorManager.GetTableProviders())
                {
                    sqlPlanBuilder.AddTableProvider(tableProvider);
                }
                configure(sqlPlanBuilder);
                return new PlanProvider(sqlPlanBuilder.GetPlan());
            });
            return builder;
        }
    }
}
