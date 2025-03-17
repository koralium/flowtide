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

using FlowtideDotNet.Connector.OpenFGA.Internal;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Zanzibar.QueryPlanner;
using OpenFga.Sdk.Model;

namespace FlowtideDotNet.Connector.OpenFGA
{
    public static class OpenFgaToFlowtide
    {
        public static Plan Convert(AuthorizationModel authorizationModel, string type, string relation, string inputTypeName, params string[]? stopAtTypes)
        {
            HashSet<string> stopTypes = new HashSet<string>();
            if (stopAtTypes != null)
            {
                foreach (var t in stopAtTypes)
                {
                    stopTypes.Add(t);
                }
            }

            var outputPlan = new Plan()
            {
                Relations = new List<Relation>()
            };

            var convertedSchema = OpenFgaToZanzibarSchema.Convert(authorizationModel);
            var zanzibarRelations = ZanzibarSchemaToQueryPlan.GenerateQueryPlan(convertedSchema, type, relation, stopTypes.ToHashSet());

            var visitor = new ZanzibarToFlowtideVisitor(
                inputTypeName,
                "user_type",
                "user_id",
                "user_relation",
                "relation",
                "object_type",
                "object_id");

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
                    "user_type",
                    "user_id",
                    "user_relation",
                    "relation",
                    "object_type",
                    "object_id"
                }
            };
            outputPlan.Relations.Add(rootRelation);


            return outputPlan;
        }
    }
}
