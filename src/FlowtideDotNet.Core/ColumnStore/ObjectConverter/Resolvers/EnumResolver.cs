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

using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Encoders;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter.Resolvers
{
    public class EnumResolver : IObjectColumnResolver
    {
        private readonly bool enumAsStrings;

        public EnumResolver(bool enumAsStrings)
        {
            this.enumAsStrings = enumAsStrings;
        }

        public bool CanHandle(ObjectConverterTypeInfo type)
        {
            return type.Type.IsEnum;
        }

        public IObjectColumnConverter GetConverter(ObjectConverterTypeInfo type, ObjectConverterResolver resolver)
        {
            if (enumAsStrings)
            {
                return new EnumStringConverter(type.Type);
            }
            return new EnumConverter(type.Type);
        }
    }
}
