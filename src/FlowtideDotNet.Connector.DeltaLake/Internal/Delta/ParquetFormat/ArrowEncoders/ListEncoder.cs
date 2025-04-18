﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using System.Diagnostics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ArrowEncoders
{
    internal class ListEncoder : IArrowEncoder
    {
        private readonly IArrowEncoder _childEncoder;
        private ListArray? _array;
        private bool _isNullColumn;

        public bool IsPartitionValueEncoder => false;


        public ListEncoder(IArrowEncoder childEncoder)
        {
            _childEncoder = childEncoder;
        }

        public void AddValue(int index, ref AddToColumnFunc func)
        {
            if (_isNullColumn)
            {
                func.AddValue(NullValue.Instance);
                return;
            }
            Debug.Assert(_array != null);

            if (_array.IsNull(index))
            {
                func.AddValue(NullValue.Instance);
                return;
            }

            var startOffset = _array.ValueOffsets[index];
            var endOffset = _array.ValueOffsets[index + 1];

            List<IDataValue> result = new List<IDataValue>();
            for (int i = startOffset; i < endOffset; i++)
            {
                AddToColumnFunc innerFunc = new AddToColumnFunc();
                _childEncoder.AddValue(i, ref innerFunc);
                result.Add(innerFunc.BoxedValue!);
            }
            func.AddValue(new ListValue(result));
        }

        public void NewBatch(IArrowArray arrowArray)
        {
            _array = (ListArray)arrowArray;
            _childEncoder.NewBatch(_array.Values);
            _isNullColumn = false;
        }

        public void NewFile(Dictionary<string, string>? partitionValues)
        {
        }

        public void NewNullBatch()
        {
            _isNullColumn = true;
        }
    }
}
