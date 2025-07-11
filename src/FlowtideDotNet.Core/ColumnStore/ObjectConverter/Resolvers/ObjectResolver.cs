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

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter.Resolvers
{
    internal class ObjectResolver : IObjectColumnResolver
    {
        public bool CanHandle(ObjectConverterTypeInfo type)
        {
            return type.TypeInfo == ObjectTypeInfoKind.Object;
        }

        public IObjectColumnConverter GetConverter(ObjectConverterTypeInfo type, ObjectConverterResolver resolver)
        {
            List<IObjectColumnConverter> converters = new List<IObjectColumnConverter>();
            foreach (var property in type.Properties)
            {
                var propertyConverter = resolver.GetConverter(property.TypeInfo);
                converters.Add(propertyConverter);
            }

            return new ObjectConverter.Converters.ObjectConverter(type, converters);
        }
    }
}
