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

using Apache.Arrow;
using Apache.Arrow.Memory;
using FlowtideDotNet.Core.ColumnStore;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.ArrowEncoding
{
    internal class Int64Encoder : IArrowColumnEncoder
    {
        private Int64Array.Builder? _builder;

        public void AddValue<T>(T value) where T : IDataValue
        {
            Debug.Assert(_builder != null);
            if (value.IsNull)
            {
                _builder.AppendNull();
                return;
            }
            _builder.Append(value.AsLong);
        }

        public IArrowArray BuildArray(MemoryAllocator memoryAllocator)
        {
            return _builder!.Build(memoryAllocator);
        }

        public void NewBatch()
        {
            _builder = new Int64Array.Builder();
        }
    }
}
