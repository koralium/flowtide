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
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter.Resolvers
{
    internal class ListResolver : IObjectColumnResolver
    {
        public bool CanHandle(ObjectConverterTypeInfo type)
        {
            return type.TypeInfo == ObjectTypeInfoKind.List;
        }

        public IObjectColumnConverter GetConverter(ObjectConverterTypeInfo type, ObjectConverterResolver resolver)
        {
            if (type.Type.IsArray)
            {
                var innerType = type.Type.GetElementType();
                return new ArrayConverter(type, resolver.GetConverter(ObjectConverterTypeInfoLookup.GetTypeInfo(innerType!)), innerType!);
            }
            else if (type.Type.IsGenericType && IsIEnumerableOfT(type.Type, out var innerType))
            {
                var genericListType = typeof(List<>).MakeGenericType(innerType);

                if (type.Type.IsAssignableFrom(genericListType))
                {
                    var innerTypeInfo = ObjectConverterTypeInfoLookup.GetTypeInfo(innerType);
                    var innerConverter = resolver.GetConverter(innerTypeInfo);
                    return new ListConverter(type, innerConverter);
                }

                var genericHashType = typeof(HashSet<>).MakeGenericType(innerType);

                if (type.Type.IsAssignableFrom(genericHashType))
                {
                    var innerTypeInfo = ObjectConverterTypeInfoLookup.GetTypeInfo(innerType);
                    var innerConverter = resolver.GetConverter(innerTypeInfo);

                    return (IObjectColumnConverter)Activator.CreateInstance(typeof(HashSetConverter<>).MakeGenericType(innerType), type, innerConverter)!;
                }
            }
            throw new NotImplementedException($"Cannot handle type {type.Type}");
        }

        private static bool IsIEnumerableOfT(Type type, [NotNullWhen(true)] out Type? elementType)
        {
            //If it is an IEnumerable passed in, check for that
            if (type.GetGenericTypeDefinition().Equals(typeof(IEnumerable<>)))
            {
                elementType = type.GetGenericArguments().First();
                return true;
            }

            var enumerableInterface = type.GetInterfaces().FirstOrDefault(x => x.IsGenericType
                   && x.GetGenericTypeDefinition() == typeof(IEnumerable<>));

            if (enumerableInterface == null)
            {
                elementType = null;
                return false;
            }

            elementType = enumerableInterface.GetGenericArguments().First();
            return true;
        }

    }
}
