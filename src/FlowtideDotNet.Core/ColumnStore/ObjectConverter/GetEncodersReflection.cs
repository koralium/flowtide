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

using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter
{
    internal static class GetEncodersReflection
    {
        public static void GetEncodersForObject(Type objectType)
        {
            var typeInfo = JsonTypeInfo.CreateJsonTypeInfo(objectType, new System.Text.Json.JsonSerializerOptions()
            {

            });
            
            if (typeInfo.Kind != JsonTypeInfoKind.Object)
            {
                throw new InvalidOperationException("Type must be an object type");
            }

            foreach(var property in typeInfo.Properties)
            {
                GetEncoder(property);
            }
        }

        private static void GetEncoder(JsonPropertyInfo jsonPropertyInfo)
        {
            var typeInfo = JsonTypeInfo.CreateJsonTypeInfo(jsonPropertyInfo.PropertyType, new System.Text.Json.JsonSerializerOptions());

            if (typeInfo.Kind == JsonTypeInfoKind.Object)
            {

            }
            else if (typeInfo.Kind == JsonTypeInfoKind.Enumerable)
            {

            }
            else if (typeInfo.Kind == JsonTypeInfoKind.Dictionary)
            {

            }
            else
            {

            }
        }
    }
}
