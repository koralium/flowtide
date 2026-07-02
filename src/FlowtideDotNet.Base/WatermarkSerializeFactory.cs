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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Base
{
    /// <summary>
    /// Provides a global registry mapping integer type IDs to <see cref="IWatermarkSerializer"/> implementations.
    /// This allows custom watermark types to be correctly serialized and deserialized across the stream engine.
    /// </summary>
    public static class WatermarkSerializeFactory
    {
        private static Dictionary<int, IWatermarkSerializer> _serializers = new Dictionary<int, IWatermarkSerializer>();
        private static object _addLock = new object();

        static WatermarkSerializeFactory()
        {
            // Register default serializers here if needed
            RegisterWatermarkType(1, new LongWatermarkValueSerializer());
        }

        /// <summary>
        /// Retrieves the registered <see cref="IWatermarkSerializer"/> for a specific type identifier.
        /// </summary>
        /// <param name="typeId">The unique integer identifier of the watermark type.</param>
        /// <returns>The registered <see cref="IWatermarkSerializer"/>.</returns>
        /// <exception cref="KeyNotFoundException">Thrown if no serializer is registered for the provided type ID.</exception>
        public static IWatermarkSerializer GetWatermarkSerializer(int typeId)
        {
            if (_serializers.TryGetValue(typeId, out var serializer))
            {
                return serializer;
            }
            throw new KeyNotFoundException($"No serializer registered for type ID {typeId}.");
        }

        /// <summary>
        /// Attempts to safely register a new <see cref="IWatermarkSerializer"/> for a specific type identifier.
        /// </summary>
        /// <param name="typeId">The unique integer identifier to associate with the serializer.</param>
        /// <param name="serializer">The serializer implementation.</param>
        /// <returns><c>true</c> if the registration was successful; <c>false</c> if the type ID was already registered.</returns>
        public static bool TryRegisterWatermarkType(int typeId, IWatermarkSerializer serializer)
        {
            lock (_addLock)
            {
                if (_serializers.ContainsKey(typeId))
                {
                    return false; // Type ID already registered
                }
                _serializers[typeId] = serializer;
                return true;
            }
        }

        /// <summary>
        /// Registers a new <see cref="IWatermarkSerializer"/> for a specific type identifier. 
        /// Throws an exception if the type ID is already in use.
        /// </summary>
        /// <param name="typeId">The unique integer identifier to associate with the serializer.</param>
        /// <param name="serializer">The serializer implementation.</param>
        /// <exception cref="ArgumentException">Thrown when a serializer is already registered to the provided type ID.</exception>
        public static void RegisterWatermarkType(int typeId, IWatermarkSerializer serializer)
        {
            lock (_addLock)
            {
                if (_serializers.ContainsKey(typeId))
                {
                    throw new ArgumentException($"A serializer for type ID {typeId} is already registered.", nameof(typeId));
                }
                _serializers[typeId] = serializer;
            }
        }
    }
}
