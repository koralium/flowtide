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

using Authzed.Api.V1;
using FlowtideDotNet.Connector.SpiceDB.Internal.SchemaParser;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Zanzibar.QueryPlanner;
using Grpc.Core;

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Provides a utility method for converting a SpiceDB permission schema into a Flowtide
    /// query plan that computes the effective subjects for a given resource type and relation.
    /// </summary>
    /// <remarks>
    /// The generated <see cref="FlowtideDotNet.Substrait.Plan"/> reads SpiceDB relationship
    /// data from a named Flowtide source table and applies the permission graph rules defined
    /// in the schema to resolve which subjects have access to which resources. Register the
    /// returned plan as a queryable view using <c>AddPlanAsView</c> and then reference it in
    /// SQL to obtain the expanded permission set.
    /// </remarks>
    public static class SpiceDbToFlowtide
    {
        /// <summary>
        /// Converts a SpiceDB schema into a Flowtide <see cref="FlowtideDotNet.Substrait.Plan"/>
        /// that, when executed, produces the effective set of subjects that have the specified
        /// relation on the given resource type.
        /// </summary>
        /// <remarks>
        /// The root relation of the returned plan produces rows with the following columns:
        /// <c>subject_type</c>, <c>subject_id</c>, <c>subject_relation</c>, <c>relation</c>,
        /// <c>resource_type</c>, and <c>resource_id</c>.
        /// </remarks>
        /// <param name="schemaText">
        /// The SpiceDB schema DSL text to parse, defining the resource types, relations, and
        /// permission rules.
        /// </param>
        /// <param name="type">
        /// The resource type to compute effective permission membership for (for example,
        /// <c>"document"</c>). Must be defined in the schema.
        /// </param>
        /// <param name="relation">
        /// The relation or permission name to expand on the resource type (for example,
        /// <c>"view"</c>). Must exist on the resource type specified by <paramref name="type"/>.
        /// </param>
        /// <param name="inputTypeName">
        /// The name of the Flowtide table that contains the SpiceDB relationship data, matching
        /// the view or table registered with the SpiceDB source connector.
        /// </param>
        /// <param name="recurseAtStopType">
        /// When <c>true</c>, the permission graph is expanded through the stop types themselves
        /// before halting. When <c>false</c> (the default), expansion stops at the boundary with
        /// stop types without recursing into them.
        /// </param>
        /// <param name="stopAtTypes">
        /// An optional set of resource type names at which recursive expansion should stop.
        /// Useful for bounding expansion of deeply nested permission hierarchies. Every type
        /// listed must be defined in the schema.
        /// </param>
        /// <returns>
        /// A <see cref="FlowtideDotNet.Substrait.Plan"/> containing the Substrait relations
        /// that compute the effective permission set for the specified type and relation.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="type"/> or any type in <paramref name="stopAtTypes"/>
        /// is not defined in the schema.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when <paramref name="relation"/> does not exist on the resource type specified
        /// by <paramref name="type"/>.
        /// </exception>
        public static Plan Convert(string schemaText, string type, string relation, string inputTypeName, bool recurseAtStopType = false, params string[]? stopAtTypes)
        {
            HashSet<string> stopTypes = new HashSet<string>();
            if (stopAtTypes != null)
            {
                foreach (var t in stopAtTypes)
                {
                    stopTypes.Add(t);
                }
            }

            var schema = SpiceDbParser.ParseSchema(schemaText);
            var zanzibarRelations = ZanzibarSchemaToQueryPlan.GenerateQueryPlan(schema, type, relation, recurseAtStopType, stopTypes);

            var visitor = new ZanzibarToFlowtideVisitor(
                inputTypeName,
                "subject_type",
                "subject_id",
                "subject_relation",
                "relation",
                "resource_type",
                "resource_id");

            var outputPlan = new Plan()
            {
                Relations = new List<Relation>()
            };

            for (int i = 0; i < zanzibarRelations.Count - 1; i++)
            {
                var flowtideReferenceRelation = visitor.Visit(zanzibarRelations[i], default);
                outputPlan.Relations.Add(flowtideReferenceRelation);
            }
            var flowtideRelation = visitor.Visit(zanzibarRelations[zanzibarRelations.Count - 1], default);

            var rootRelation = new RootRelation()
            {
                Input = flowtideRelation,
                Names = new List<string>()
                {
                    "subject_type",
                    "subject_id",
                    "subject_relation",
                    "relation",
                    "resource_type",
                    "resource_id"
                }
            };

            outputPlan.Relations.Add(rootRelation);

            return outputPlan;
        }

        /// <summary>
        /// Reads the SpiceDB schema from the provided gRPC channel and converts a specific permission relation into a Flowtide plan using the <see cref="Convert"/> method.
        /// </summary>
        /// <param name="channel"></param>
        /// <param name="type"></param>
        /// <param name="relation"></param>
        /// <param name="inputTypeName"></param>
        /// <param name="recurseAtStopType"></param>
        /// <param name="stopAtTypes"></param>
        /// <returns></returns>
        public static async Task<Plan> ConvertAsync(
            ChannelBase channel,
            Metadata headers,
            string type, 
            string relation, 
            string inputTypeName, 
            bool recurseAtStopType = false,
            params string[]? stopAtTypes)
        {
            SchemaService.SchemaServiceClient client = new SchemaService.SchemaServiceClient(channel);
            var schemaResponse = await client.ReadSchemaAsync(new ReadSchemaRequest(), headers: headers);
            return Convert(schemaResponse.SchemaText, type, relation, inputTypeName, recurseAtStopType, stopAtTypes);
        }
    }
}
