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

using OpenFga.Sdk.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    public class ModelParser
    {
        public void Parse(AuthorizationModel authorizationModel, string type, string relation)
        {
            var typeDefinition = authorizationModel.TypeDefinitions.Find(x => x.Type.Equals(type, StringComparison.OrdinalIgnoreCase));

            if (typeDefinition == null)
            {
                throw new InvalidOperationException($"Type {type} not found in authorization model");
            }
            if (typeDefinition.Relations == null)
            {
                throw new InvalidOperationException($"Type {type} has no relations defined");
            }
            if (!typeDefinition.Relations.TryGetValue(relation, out var relationDefinition))
            {
                throw new InvalidOperationException($"Relation {relation} not found in type {type}");
            }
            if (relationDefinition.This != null)
            {
                if (!typeDefinition.Metadata.Relations.TryGetValue(relation, out var thisRelation))
                {
                    throw new InvalidOperationException($"Relation {relation} not found in type {type}");
                }
            }
        }
    }
}
