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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    /// <summary>
    /// Temporary class to help convert from a list of row events to batch data.
    /// This class can be removed when all the code is refactored to use the new event batch data.
    /// </summary>
    internal static class RowEventToEventBatchData
    {

        // Allot of boxing in this method, but it is temporary until flxvalue is phased out.
        public static IDataValue FlxValueToDataValue(FlxValue flxValue)
        {
            switch (flxValue.ValueType)
            {
                case FlexBuffers.Type.Null:
                    return new NullValue();
                case FlexBuffers.Type.Bool:
                    return new BoolValue(flxValue.AsBool);
                case FlexBuffers.Type.Int:
                    return new Int64Value(flxValue.AsLong);
                case FlexBuffers.Type.String:
                    return new StringValue(flxValue.AsFlxString.Span.ToArray());
                case FlexBuffers.Type.Decimal:
                    return new DecimalValue(flxValue.AsDecimal);
                case FlexBuffers.Type.Float:
                    return new DoubleValue(flxValue.AsDouble);
                case FlexBuffers.Type.Blob:
                    var blob = flxValue.AsBlob;
                    return new BinaryValue(blob.ToArray());
                case FlexBuffers.Type.Map:
                    return MapToDataValue(flxValue);
                case FlexBuffers.Type.Vector:
                    return ListToDataValue(flxValue);
            }
            throw new NotImplementedException();
        }

        private static IDataValue ListToDataValue(FlxValue flxValue)
        {
            var list = flxValue.AsVector;

            List<IDataValue> dataValues = new List<IDataValue>();
            for (int i = 0; i < list.Length; i++)
            {
                var innerVal = FlxValueToDataValue(list.Get(i));
                dataValues.Add(innerVal);
            }
            return new ListValue(dataValues);
        }

        private static IDataValue MapToDataValue(FlxValue flxValue)
        {
            var map = flxValue.AsMap;
            
            List<KeyValuePair<IDataValue, IDataValue>> dataMap = new List<KeyValuePair<IDataValue, IDataValue>>();
            foreach (var kv in map)
            {
                var innerVal = FlxValueToDataValue(kv.Value);
                dataMap.Add(new KeyValuePair<IDataValue, IDataValue>(new StringValue(kv.Key), innerVal));
            }
            return new MapValue(dataMap);
        }

        public static FlxValue DataValueToFlxValue(IDataValue dataValue)
        {
            switch (dataValue.Type)
            {
                case ArrowTypeId.Null:
                    return FlxValue.Null;
                case ArrowTypeId.Int64:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(dataValue.AsLong));
                case ArrowTypeId.Boolean:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(dataValue.AsBool));
                case ArrowTypeId.String:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Encoding.UTF8.GetString(dataValue.AsString.Span)));
                case ArrowTypeId.Decimal128:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(dataValue.AsDecimal));
                case ArrowTypeId.Double:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(dataValue.AsDouble));
                case ArrowTypeId.Binary:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(dataValue.AsBinary.ToArray()));
                case ArrowTypeId.Timestamp:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(dataValue.AsTimestamp.ticks));
                case ArrowTypeId.Map:
                    return MapToFlxValue(dataValue);
                case ArrowTypeId.List:
                    return ListToFlxValue(dataValue);
            }
            throw new NotImplementedException();
        }
        
        private static FlxValue ListToFlxValue(IDataValue value)
        {
            var list = value.AsList;

            var bytes = FlexBufferBuilder.Vector(v =>
            {
                int listCount = list.Count;
                for (int i = 0; i < listCount; i++)
                {
                    var innerVal = DataValueToFlxValue(list.GetAt(i));
                    v.Add(innerVal);
                }
            });

            return FlxValue.FromBytes(bytes);
        }

        private static FlxValue MapToFlxValue(IDataValue value)
        {
            var map = value.AsMap;

            var bytes = FlexBufferBuilder.Map(m =>
            {
                foreach (var kv in map)
                {
                    var innerVal = DataValueToFlxValue(kv.Value);
                    m.Add(kv.Key.ToString()!, innerVal);
                }
            });
            return FlxValue.FromBytes(bytes);
        }

        public static List<RowEvent> EventBatchWeightedToRowEvents(EventBatchWeighted eventBatch)
        {
            List<RowEvent> rowEvents = new List<RowEvent>();
            for (int i = 0; i < eventBatch.Weights.Count; i++)
            {
                var weight = eventBatch.Weights[i];
                var iteration = eventBatch.Iterations[i];
                FlxValue[] columns = new FlxValue[eventBatch.EventBatchData.Columns.Count];
                for (int c = 0; c < eventBatch.EventBatchData.Columns.Count; c++)
                {
                    columns[c] = DataValueToFlxValue(eventBatch.EventBatchData.Columns[c].GetValueAt(i, default));
                }
                rowEvents.Add(new RowEvent(weight, iteration, new ArrayRowData(columns)));
            }
            return rowEvents;
        }

        public static RowEvent RowReferenceToRowEvent(int weight, uint iteration, ColumnRowReference columnRowReference)
        {
            FlxValue[] columns = new FlxValue[columnRowReference.referenceBatch.Columns.Count];
            for (int c = 0; c < columnRowReference.referenceBatch.Columns.Count; c++)
            {
                columns[c] = DataValueToFlxValue(columnRowReference.referenceBatch.Columns[c].GetValueAt(columnRowReference.RowIndex, default));
            }
            return new RowEvent(weight, iteration, new ArrayRowData(columns));
        }

        public static EventBatchWeighted ConvertToEventBatchData(List<RowEvent> rowEvents, int columnCount)
        {
            var batchmanager = GlobalMemoryManager.Instance;
            PrimitiveList<int> weights = new PrimitiveList<int>(batchmanager);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(batchmanager);

            IColumn[] columns = new IColumn[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                columns[i] = Column.Create(batchmanager);
            }

            foreach(var e in rowEvents)
            {
                weights.Add(e.Weight);
                iterations.Add(e.Iteration);
                for (int i = 0; i < columnCount; i++)
                {
                    var dataValue = FlxValueToDataValue(e.GetColumn(i));
                    columns[i].Add(dataValue);
                }
            }

            var eventBatchData = new EventBatchData(columns);

            return new EventBatchWeighted(weights, iterations, eventBatchData);
        }
    }
}
