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
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Resolvers;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter
{
    public class ObjectConverterResolver
    {
        public static readonly ObjectConverterResolver Default = new ObjectConverterResolver();

        private List<IObjectColumnResolver> _resolvers = GetResolvers();

        private static List<IObjectColumnResolver> GetResolvers()
        {
            return new List<IObjectColumnResolver>()
            {
                new NullableResolver(),
                new Int64Resolver(),
                new StringResolver(),
                new DateTimeResolver(),
                new DateTimeOffsetResolver(),
                new BoolResolver(),
                new DoubleResolver(),
                new FloatResolver(),
                new DecimalResolver(),
                new ByteArrayResolver(),
                new MemoryByteResolver(),
                new ReadOnlyMemoryResolver(),
                new EnumResolver(false),
                new GuidResolver(),
                new CharResolver(),

                // Complex types
                new ObjectResolver(),
                new DictionaryResolver(),
                new ListResolver(),
                new JsonElementResolver(),
                new UnionResolver(),
            };
        }

        public void AppendResolver(IObjectColumnResolver resolver)
        {
            _resolvers.Add(resolver);
        }

        public void PrependResolver(IObjectColumnResolver resolver)
        {
            _resolvers.Insert(0, resolver);
        }

        public void ClearResolvers()
        {
            _resolvers.Clear();
        }

        public IObjectColumnConverter GetConverter(ObjectConverterTypeInfo type)
        {
            foreach (var resolver in _resolvers)
            {
                if (resolver.CanHandle(type))
                {
                    return resolver.GetConverter(type, this);
                }
            }

            throw new InvalidOperationException($"No resolver found for type {type.Type}");
        }
    }
}
