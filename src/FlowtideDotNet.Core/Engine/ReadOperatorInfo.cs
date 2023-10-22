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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Engine
{
    public class ReadOperatorInfo
    {
        public IStreamIngressVertex IngressVertex { get; }

        /// <summary>
        /// Normalization operator that takes out only changed rows from the source.
        /// It groups the rows based on a primary key row.
        /// </summary>
        public NormalizationRelation? NormalizationRelation { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ingressVertex"></param>
        /// <param name="normalizationRelation">Pass a normalization relation here that will be added in front of the read relation.
        /// </param>
        public ReadOperatorInfo(IStreamIngressVertex ingressVertex, NormalizationRelation? normalizationRelation = default)
        {
            IngressVertex = ingressVertex;
            NormalizationRelation = normalizationRelation;
        }
    }
}
