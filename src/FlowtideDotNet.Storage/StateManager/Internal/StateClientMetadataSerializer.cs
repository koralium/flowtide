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

using System.Buffers;
using System.Diagnostics;
using System.Text.Json;

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    internal static class StateClientMetadataSerializer
    {
        public static StateClientMetadata<T> Deserialize<T>(ReadOnlyMemory<byte> bytes, int length)
        {
            var slice = bytes.Span.Slice(0, length);
            var reader = new Utf8JsonReader(slice);
            var deserializedValue = JsonSerializer.Deserialize<StateClientMetadata<T>>(ref reader);
            Debug.Assert(deserializedValue != null);
            return deserializedValue;
        }

        public static byte[] Serialize<T>(in StateClientMetadata<T> value)
        {
            using MemoryStream memoryStream = new MemoryStream();
            JsonSerializer.Serialize(memoryStream, value);
            var arr = memoryStream.ToArray();
            return arr;
        }
    }
}
