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

using FlowtideDotNet.Core.ColumnStore.DataValues;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Json
{
    public class DataValueJsonWriter : DataValueVisitor
    {
        private readonly ArrayBufferWriter<byte> _writer;
        private readonly Utf8JsonWriter jsonWriter;

        public ReadOnlyMemory<byte> WrittenMemory => _writer.WrittenMemory;

        public void Reset()
        {
            _writer.ResetWrittenCount();
            jsonWriter.Reset();
        }

        public DataValueJsonWriter()
        {
            _writer = new ArrayBufferWriter<byte>();
            this.jsonWriter = new Utf8JsonWriter(_writer);
        }

        public void Flush()
        {
            jsonWriter.Flush();
        }

        public override void VisitInt64Value(ref readonly Int64Value int64Value)
        {
            jsonWriter.WriteNumberValue(int64Value.AsLong);
        }

        public override void VisitStringValue(ref readonly StringValue stringValue)
        {
            jsonWriter.WriteStringValue(stringValue.AsString.Span);
        }

        public override void VisitBinaryValue(ref readonly BinaryValue binaryValue)
        {
            jsonWriter.WriteBase64StringValue(binaryValue.AsBinary);
        }

        public override void VisitBoolValue(ref readonly BoolValue boolValue)
        {
            jsonWriter.WriteBooleanValue(boolValue.AsBool);
        }

        public override void VisitDecimalValue(ref readonly DecimalValue decimalValue)
        {
            jsonWriter.WriteNumberValue(decimalValue.AsDecimal);
        }

        public override void VisitDoubleValue(ref readonly DoubleValue doubleValue)
        {
            jsonWriter.WriteNumberValue(doubleValue.AsDouble);
        }

        public override void VisitListValue(ref readonly ListValue listValue)
        {
            jsonWriter.WriteStartArray();
            for (int i = 0; i < listValue.Count; i++)
            {
                listValue.GetAt(i).Accept(this);
            }
            jsonWriter.WriteEndArray();
        }

        public override void VisitMapValue(ref readonly MapValue mapValue)
        {
            jsonWriter.WriteStartObject();
            var mapLength = mapValue.GetLength();
            for (int i = 0; i < mapLength; i++)
            {
                var keyValue = mapValue.GetKeyAt(in i);
                jsonWriter.WritePropertyName(keyValue.ToString()!);
                var val = mapValue.GetValueAt(in i);
                val.Accept(this);
            }
            jsonWriter.WriteEndObject();
        }

        public override void VisitNullValue(ref readonly NullValue nullValue)
        {
            jsonWriter.WriteNullValue();
        }

        public override void VisitTimestampTzValue(ref readonly TimestampTzValue timestampTzValue)
        {
            jsonWriter.WriteStringValue(timestampTzValue.ToDateTimeOffset());
        }

        public override void VisitReferenceListValue(ref readonly ReferenceListValue referenceListValue)
        {
            jsonWriter.WriteStartArray();
            for (int i = 0; i < referenceListValue.Count; i++)
            {
                referenceListValue.GetAt(i).Accept(this);
            }
            jsonWriter.WriteEndArray();
        }

        public override void VisitReferenceMapValue(ref readonly ReferenceMapValue referenceMapValue)
        {
            jsonWriter.WriteStartObject();
            var mapLength = referenceMapValue.GetLength();
            for (int i = 0; i < mapLength; i++)
            {
                var keyValue = referenceMapValue.GetKeyAt(in i);
                jsonWriter.WritePropertyName(keyValue.ToString()!);
                var val = referenceMapValue.GetValueAt(in i);
                val.Accept(this);
            }
            jsonWriter.WriteEndObject();
        }

        public override void VisitReferenceStructValue(ref readonly ReferenceStructValue referenceStructValue)
        {
            jsonWriter.WriteStartObject();
            var structLength = referenceStructValue.Header.Count;

            for (int i = 0; i < structLength; i++)
            {
                var columnName = referenceStructValue.Header.GetColumnNameUtf8(i);
                jsonWriter.WritePropertyName(columnName);
                var val = referenceStructValue.GetAt(i);
                val.Accept(this);
            }
            jsonWriter.WriteEndObject();
        }

        public override void VisitStructValue(ref readonly StructValue structValue)
        {
            jsonWriter.WriteStartObject();
            var structLength = structValue.Header.Count;

            for (int i = 0; i < structLength; i++)
            {
                var columnName = structValue.Header.GetColumnNameUtf8(i);
                jsonWriter.WritePropertyName(columnName);
                var val = structValue.GetAt(i);
                val.Accept(this);
            }
            jsonWriter.WriteEndObject();
        }
    }
}
