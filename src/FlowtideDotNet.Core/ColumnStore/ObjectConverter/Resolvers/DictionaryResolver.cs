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

using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Encoders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter.Resolvers
{
    internal class DictionaryResolver : IObjectColumnResolver
    {
        public bool CanHandle(ObjectConverterTypeInfo type)
        {
            return type.TypeInfo == ObjectTypeInfoKind.Dictionary;
        }

        public IObjectColumnConverter GetConverter(ObjectConverterTypeInfo type, ObjectConverterResolver resolver)
        {
            var interfaces = type.Type.GetInterfaces();

            // Find IDictionary interface
            var dictionaryInterface = interfaces.FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>));

            if (dictionaryInterface == null)
            {
                throw new ArgumentException("Dictionary type must implement IDictionary interface");
            }

            var genericArgs = dictionaryInterface.GetGenericArguments();
            var keyType = genericArgs[0];
            var valueType = genericArgs[1];

            // Get converters
            var keyConverter = resolver.GetConverter(ObjectConverterTypeInfoLookup.GetTypeInfo(keyType));
            var valueConverter = resolver.GetConverter(ObjectConverterTypeInfoLookup.GetTypeInfo(valueType));

            // Create new dictionary converter with the generic types
            return (IObjectColumnConverter)Activator.CreateInstance(typeof(DictionaryConverter<,>).MakeGenericType(keyType, valueType), type, keyConverter, valueConverter)!;
        }
    }
}
