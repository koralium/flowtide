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

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter
{
    public class ObjectConverterTypeInfo
    {
        public Type Type { get; }
        public ObjectTypeInfoKind TypeInfo { get; }
        public IReadOnlyList<ObjectConverterPropertyInfo> Properties { get; }
        public Func<object>? CreateObject { get; }
        public object? DefaultValue { get; }

        public ObjectConverterTypeInfo(Type type, ObjectTypeInfoKind typeInfo, IReadOnlyList<ObjectConverterPropertyInfo> properties, Func<object>? createObject, object? defaultValue)
        {
            Type = type;
            TypeInfo = typeInfo;
            Properties = properties;
            CreateObject = createObject;
            DefaultValue = defaultValue;
        }
    }
}
