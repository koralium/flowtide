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
    internal class Int64Encoder : IArrowEncoder
    {
        private Int64Array? _array;
        public void AddValue(int index, ref AddToColumnFunc func)
        {
            Debug.Assert(_array != null);

            var val = _array.GetValue(index);
            if (!val.HasValue)
            {
                func.AddValue(NullValue.Instance);
            }
            else
            {
                func.AddValue(new Int64Value(val.Value));
            }
        }

        public void NewBatch(IArrowArray arrowArray)
        {
            if (arrowArray is Int64Array int64Array)
            {
                _array = int64Array;
            }
            else
            {
                throw new ArgumentException("Expected Int64 array", nameof(arrowArray));
            }
        }

        public void NewFile(Dictionary<string, string>? partitionValues)
        {
        }
    }
}
