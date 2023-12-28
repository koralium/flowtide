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

using FlexBuffers;
using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core
{
    internal struct VectorRowData : IRowData
    {
        private readonly FlxVector m_vector;

        public VectorRowData(FlxVector vector)
        {
            this.m_vector = vector;
        }

        public int Length => m_vector.Length;

        public FlxValue GetColumn(int index)
        {
            return m_vector.Get(index);
        }

        public FlxValueRef GetColumnRef(scoped in int index)
        {
            return m_vector.GetRef(index);
        }
    }
}
