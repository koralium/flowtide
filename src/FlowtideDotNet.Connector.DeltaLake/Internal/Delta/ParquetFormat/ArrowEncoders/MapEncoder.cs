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
    internal class MapEncoder : IArrowEncoder
    {
        private MapArray? _array;
        private readonly IArrowEncoder _keyEncoder;
        private readonly IArrowEncoder _valueEncoder;

        public bool IsPartitionValueEncoder => false;

        public MapEncoder(IArrowEncoder keyEncoder, IArrowEncoder valueEncoder)
        {
            this._keyEncoder = keyEncoder;
            this._valueEncoder = valueEncoder;
        }

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
                return;
            }

            var startOffset = _array.ValueOffsets[index];
            var endOffset = _array.ValueOffsets[index + 1];

            List<KeyValuePair<IDataValue, IDataValue>> result = new List<KeyValuePair<IDataValue, IDataValue>>();

            for (int i = startOffset; i < endOffset; i++)
            {
                AddToColumnFunc innerFunc = new AddToColumnFunc();
                _keyEncoder.AddValue(i, ref innerFunc);
                var key = innerFunc.BoxedValue!;

                innerFunc = new AddToColumnFunc();
                _valueEncoder.AddValue(i, ref innerFunc);
                var value = innerFunc.BoxedValue!;

                result.Add(new KeyValuePair<IDataValue, IDataValue>(key, value));
            }

            func.AddValue(new MapValue(result));
        }

        public void NewBatch(IArrowArray arrowArray)
        {
            if (arrowArray is MapArray mapArray)
            {
                _array = mapArray;
                _keyEncoder.NewBatch(mapArray.Keys);
                _valueEncoder.NewBatch(mapArray.Values);
            }
            else
            {
                throw new ArgumentException("Expected Map array", nameof(arrowArray));
            }
        }

        public void NewFile(Dictionary<string, string>? partitionValues)
        {
        }

        public void NewNullBatch()
        {
            _array = null;
        }
    }
}
