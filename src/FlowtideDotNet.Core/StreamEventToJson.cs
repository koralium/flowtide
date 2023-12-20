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
using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core
{
    public class StreamEventToJson
    {
        private readonly List<string> names;
        private readonly List<JsonEncodedText> propertyNames;

        public StreamEventToJson(List<string> names)
        {
            this.names = names;
            propertyNames = new List<JsonEncodedText>(names.Count);

            foreach(var name in names)
            {
                propertyNames.Add(JsonEncodedText.Encode(name));
            }
        }

        public void Write(in Stream stream, in StreamEvent streamEvent)
        {
            var writer = new Utf8JsonWriter(stream);
            writer.WriteStartObject();
            for(int i = 0; i < streamEvent.Vector.Length; i++)
            {
                if (ShouldWriteColumn(i))
                {
                    WritePropertyName(writer, i);
                    WriteColumnValue(writer, streamEvent.Vector.GetRef(i), i);
                }
            }
            writer.WriteEndObject();
            writer.Flush();
        }

        protected virtual bool ShouldWriteColumn(int index) => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void WritePropertyName(in Utf8JsonWriter writer, in int index)
        {
            writer.WritePropertyName(propertyNames[index]);
        }

        protected virtual void WriteColumnValue(in Utf8JsonWriter writer, in FlxValueRef val, in int index)
        {
            Write(writer, val);
        }

        protected void Write(in Utf8JsonWriter writer, in FlxValueRef flxValue)
        {
            if (flxValue.IsNull)
            {
                writer.WriteNullValue();
                return;
            }
            if (flxValue.ValueType == FlexBuffers.Type.Bool)
            {
                writer.WriteBooleanValue(flxValue.AsBool);
                return;
            }
            if (flxValue.ValueType == FlexBuffers.Type.Int)
            {
                writer.WriteNumberValue(flxValue.AsLong);
                return;
            }
            if (flxValue.ValueType == FlexBuffers.Type.Uint)
            {
                writer.WriteNumberValue(flxValue.AsULong);
                return;
            }
            if (flxValue.ValueType == FlexBuffers.Type.Float)
            {
                writer.WriteNumberValue(flxValue.AsDouble);
                return;
            }
            if (flxValue.ValueType == FlexBuffers.Type.String)
            {
                writer.WriteStringValue(flxValue.AsFlxString.Span);
                return;
            }
            if (flxValue.ValueType == FlexBuffers.Type.Decimal)
            {
                writer.WriteNumberValue(flxValue.AsDecimal);
                return;
            }
            if (flxValue.ValueType == FlexBuffers.Type.Vector)
            {
                var vec = flxValue.AsVector;
                writer.WriteStartArray();
                for (int i = 0; i < vec.Length; i++)
                {
                    Write(writer, vec[i]);
                }
                writer.WriteEndArray();
            }
            if (flxValue.ValueType == FlexBuffers.Type.Map)
            {
                var map = flxValue.AsMap;
                writer.WriteStartObject();
                for (int i = 0; i < map.Keys.Length; i++)
                {
                    writer.WritePropertyName(map.Keys[i].AsFlxString.Span);
                    Write(writer, map.Values[i]);
                }
                writer.WriteEndObject();
            }
        }

    }
}
