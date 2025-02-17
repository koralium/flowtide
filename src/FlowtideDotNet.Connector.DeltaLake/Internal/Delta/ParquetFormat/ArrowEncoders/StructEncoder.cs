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
    internal class StructEncoder : IArrowEncoder
    {
        private readonly List<IArrowEncoder> _encoders;
        private readonly List<IDataValue> _propertyNames;
        private StructArray? _array;

        public bool IsPartitionValueEncoder => false;

        public StructEncoder(List<IArrowEncoder> encoders, List<IDataValue> propertyNames)
        {
            this._encoders = encoders;
            this._propertyNames = propertyNames;
        }
        public void AddValue(int index, ref AddToColumnFunc func)
        {
            Debug.Assert(_array != null);

            if (_array.IsNull(index))
            {
                func.AddValue(NullValue.Instance);
                return;
            }

            List<KeyValuePair<IDataValue, IDataValue>> result = new List<KeyValuePair<IDataValue, IDataValue>>();

            for (int i = 0; i < _encoders.Count; i++)
            {
                AddToColumnFunc innerFunc = new AddToColumnFunc();
                _encoders[i].AddValue(index, ref innerFunc);
                result.Add(new KeyValuePair<IDataValue, IDataValue>(_propertyNames[i], innerFunc.BoxedValue!));
            }

            func.AddValue(new MapValue(result));
        }

        public void NewBatch(IArrowArray arrowArray)
        {
            if (arrowArray is StructArray structArray)
            {
                _array = structArray;
                for (int i = 0; i < _encoders.Count; i++)
                {
                    _encoders[i].NewBatch(structArray.Fields[i]);
                }
            }
            else
            {
                throw new ArgumentException("Expected struct array", nameof(arrowArray));
            }
        }

        public void NewFile(Dictionary<string, string>? partitionValues)
        {
        }
    }
}
