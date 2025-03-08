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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ArrowEncoders
{
    internal class BinaryEncoder : IArrowEncoder
    {
        private BinaryArray? _array;

        public bool IsPartitionValueEncoder => false;

        public void AddValue(int index, ref AddToColumnFunc func)
        {
            if (_array == null)
            {
                func.AddValue(NullValue.Instance);
                return;
            }
            
            if (_array.IsNull(index))
            {
                func.AddValue(NullValue.Instance);
            }
            else
            {
                var val = _array.ValueBuffer.Memory.Slice(_array.ValueOffsets[index], _array.GetValueLength(index));
                func.AddValue(new BinaryValue(val));
            }
        }

        public void NewBatch(IArrowArray arrowArray)
        {
            if (arrowArray is BinaryArray arr)
            {
                _array = arr;
            }
            else
            {
                throw new ArgumentException("Expected boolean array", nameof(arrowArray));
            }
        }

        public void NewFile(Dictionary<string, string>? partitionValues)
        {
        }

        public void NewNullBatch()
        {
            _array = default;
        }
    }
}
