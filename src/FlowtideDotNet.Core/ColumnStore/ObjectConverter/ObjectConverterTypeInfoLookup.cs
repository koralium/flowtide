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

using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Encoders;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter
{
    internal static class ObjectConverterTypeInfoLookup
    {
        private static ObjectTypeInfoKind ConvertKind(JsonTypeInfoKind kind)
        {
            return kind switch
            {
                JsonTypeInfoKind.Object => ObjectTypeInfoKind.Object,
                JsonTypeInfoKind.Dictionary => ObjectTypeInfoKind.Dictionary,
                JsonTypeInfoKind.Enumerable => ObjectTypeInfoKind.List,
                JsonTypeInfoKind.None => ObjectTypeInfoKind.None,
                _ => throw new InvalidOperationException("Unknown kind")
            };
        }

        public static ObjectConverterTypeInfo GetTypeInfo(Type type)
        {
            var opt = new System.Text.Json.JsonSerializerOptions()
            {
                IgnoreReadOnlyProperties = false,
                TypeInfoResolver = new DefaultJsonTypeInfoResolver()
            };
            opt.Converters.Clear();
            var resolver = new DefaultJsonTypeInfoResolver();
            var typeInfo = resolver.GetTypeInfo(type, opt);
            
            if (typeInfo.Properties.Count == 0)
            {
                return new ObjectConverterTypeInfo(type, ConvertKind(typeInfo.Kind), Array.Empty<ObjectConverterPropertyInfo>(), typeInfo.CreateObject);
            }
            else
            {
                List<ObjectConverterPropertyInfo> properties = new List<ObjectConverterPropertyInfo>();
                foreach (var property in typeInfo.Properties)
                {
                    properties.Add(new ObjectConverterPropertyInfo(property.Name, property.Get, property.Set, GetTypeInfo(property.PropertyType)));
                }

                return new ObjectConverterTypeInfo(type, ConvertKind(typeInfo.Kind), properties, typeInfo.CreateObject);
            }
        }
    }
}
