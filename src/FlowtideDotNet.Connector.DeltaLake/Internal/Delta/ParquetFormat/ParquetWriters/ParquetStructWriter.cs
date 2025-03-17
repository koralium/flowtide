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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters
{
    internal class ParquetStructWriter : IParquetWriter
    {
        private readonly List<KeyValuePair<string, IParquetWriter>> propertyWriters;
        private List<int> order;
        ArrowBuffer.BitmapBuilder? _nullBitmap;
        private int _nullCount;


        public ParquetStructWriter(IEnumerable<KeyValuePair<string, IParquetWriter>> propertyWriters)
        {
            var keys = propertyWriters.Select(x => x.Key).ToList();
            this.propertyWriters = propertyWriters.OrderBy(x => x.Key).ToList();

            order = new List<int>();
            foreach (var key in keys)
            {
                for (int i = 0; i < this.propertyWriters.Count; i++)
                {
                    if (this.propertyWriters[i].Key == key)
                    {
                        order.Add(i);
                        break;
                    }
                }
            }
        }

        public void CopyArray(IArrowArray array, int globalOffset, IDeleteVector deleteVector, int index, int count)
        {
            if (array is StructArray arr)
            {
                for (int j = 0; j < propertyWriters.Count; j++)
                {
                    var field = arr.Fields[j];
                    propertyWriters[order[j]].Value.CopyArray(field, globalOffset, deleteVector, index, count);
                }
                for (int i = index; i < (index + count); i++)
                {
                    if (deleteVector.Contains(globalOffset + i))
                    {
                        continue;
                    }

                    if (arr.IsNull(i))
                    {
                        WriteNull();
                    }
                }
                return;
            }
            throw new NotImplementedException();
        }

        public IArrowArray GetArray()
        {
            Debug.Assert(_nullBitmap != null);
            List<IArrowArray> arrays = new List<IArrowArray>();
            List<Apache.Arrow.Field> fields = new List<Field>();

            for (int i = 0; i < order.Count; i++)
            {
                var writer =  propertyWriters[order[i]];
                var arr = writer.Value.GetArray();
                arrays.Add(arr);
                fields.Add(new Apache.Arrow.Field(writer.Key, arr.Data.DataType, true));
            }

            var structType = new Apache.Arrow.Types.StructType(fields);

            return new StructArray(structType, arrays[0].Length, arrays, _nullBitmap.Build());
        }

        public IStatisticsComparer? GetStatisticsComparer()
        {
            List<KeyValuePair<string, IStatisticsComparer>> statisticsComparers = new List<KeyValuePair<string, IStatisticsComparer>>();
            foreach(var writer in propertyWriters)
            {
                var comparer = writer.Value.GetStatisticsComparer();
                if (comparer != null)
                {
                    statisticsComparers.Add(new KeyValuePair<string, IStatisticsComparer>(writer.Key, comparer));
                }
            }
            return new StructStatisticsComparer(statisticsComparers, _nullCount);
        }

        public void NewBatch()
        {
            _nullBitmap = new ArrowBuffer.BitmapBuilder();
            _nullCount++;
            foreach(var writer in propertyWriters)
            {
                writer.Value.NewBatch();
            }
        }

        public void WriteNull()
        {
            Debug.Assert(_nullBitmap != null);
            _nullCount++;
            _nullBitmap.Append(false);
        }

        public void WriteValue<T>(T value) where T : IDataValue
        {
            Debug.Assert(_nullBitmap != null);
            if (value.IsNull)
            {
                _nullCount++;
                _nullBitmap.Append(false);
                foreach(var writer in propertyWriters)
                {
                    writer.Value.WriteNull();
                }
                return;
            }

            var mapValue = value.AsMap;

            _nullBitmap.Append(true);
            var length = mapValue.GetLength();

            for (int i = 0; i < length; i++)
            {
                var key = mapValue.GetKeyAt(i);
                var keyString = key.AsString.ToString();
                var val = mapValue.GetValueAt(i);

                if (propertyWriters[i].Key == keyString)
                {
                    propertyWriters[i].Value.WriteValue(val);
                }
                else
                {
                    propertyWriters[i].Value.WriteNull();
                }
            }
        }
    }
}
