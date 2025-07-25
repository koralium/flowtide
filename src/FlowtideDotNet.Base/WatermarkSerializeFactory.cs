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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Base
{
    public static class WatermarkSerializeFactory
    {
        private static Dictionary<int, IWatermarkSerializer> _serializers = new Dictionary<int, IWatermarkSerializer>();
        private static object _addLock = new object();

        static WatermarkSerializeFactory()
        {
            // Register default serializers here if needed
            RegisterWatermarkType(1, new LongWatermarkValueSerializer());
        }

        public static IWatermarkSerializer GetWatermarkSerializer(int typeId)
        {
            if (_serializers.TryGetValue(typeId, out var serializer))
            {
                return serializer;
            }
            throw new KeyNotFoundException($"No serializer registered for type ID {typeId}.");
        }


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
