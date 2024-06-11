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

        public static IDataValue FlxValueToDataValue(FlxValue flxValue)
        {
            switch (flxValue.ValueType)
            {
                case FlexBuffers.Type.Bool:
                    return new BoolValue(flxValue.AsBool);
                case FlexBuffers.Type.Int:
                    return new Int64Value(flxValue.AsLong);
                case FlexBuffers.Type.String:
                    return new StringValue(flxValue.AsFlxString.Span.ToArray());
                case FlexBuffers.Type.Decimal:
                    return new DecimalValue(flxValue.AsDecimal);
            }
            throw new NotImplementedException();
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
                    m.Add(kv.Key, innerVal);
                }
            });
            return FlxValue.FromBytes(bytes);
        }

        public static EventBatchData ConvertToEventBatchData(List<RowEvent> rowEvents, int columnCount)
        {
            List<Column> columns = new List<Column>();
            for (int i = 0; i < columnCount; i++)
            {
                columns.Add(new Column());
            }

            foreach(var e in rowEvents)
            {
                for (int i = 0; i < columnCount; i++)
                {
                    var dataValue = FlxValueToDataValue(e.GetColumn(i));
                    columns[i].Add(dataValue);
                }
            }

            var eventBatchData = new EventBatchData(columns);

            return eventBatchData;
        }
    }
}
