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
using OpenFga.Sdk.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

            var zanzibarRelation = new FlowtideZanzibarConverter(authorizationModel, stopTypes.ToHashSet()).Parse(type, relation);
            var visitor = new ZanzibarToFlowtideVisitor(inputTypeName);
            var flowtideRelation = visitor.Visit(zanzibarRelation, default);

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

            var outputPlan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    rootRelation
                }
            };

            return outputPlan;
        }
    }
}
