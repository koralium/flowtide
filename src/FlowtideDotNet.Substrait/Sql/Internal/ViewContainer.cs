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

using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal class ViewContainer
    {
        public ViewContainer(EmitData emitData, int relationId, int outputLength)
        {
            EmitData = emitData;
            RelationId = relationId;
            OutputLength = outputLength;
        }

        public int RelationId { get; }

        public EmitData EmitData { get; }

        public int OutputLength { get; }
    }
}
