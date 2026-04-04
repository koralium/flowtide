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

using FlowtideDotNet.Zanzibar.QueryPlanner.Models;

namespace FlowtideDotNet.Zanzibar.QueryPlanner
{
    public static class ZanzibarSchemaToQueryPlan
    {
        public static List<ZanzibarRelation> GenerateQueryPlan(ZanzibarSchema schema, string type, string relation, bool recurseAtStopType, HashSet<string> stopTypes)
        {
            if (!schema.Types.ContainsKey(type))
            {
                throw new ArgumentException($"Type {type} is not in the schema");
            }
            if (stopTypes.Count > 0)
            {
                foreach (var stopType in stopTypes)
                {
                    if (!schema.Types.ContainsKey(stopType))
                    {
                        throw new ArgumentException($"Stop type {stopType} is not in the schema");
                    }
                }
            }
            var result = new ZanzibarFlowtideConvertVisitor(schema, stopTypes, recurseAtStopType, false, default).Parse(type, relation);

            return result;
        }
    }
}
