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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class MapType : SchemaBaseType
    {
        public override SchemaType Type => SchemaType.Map;

        public SchemaBaseType KeyType { get; }

        public SchemaBaseType ValueType { get; }

        public bool ValueContainsNull { get; }

        public MapType(SchemaBaseType keyType, SchemaBaseType valueType, bool valueContainsNull)
        {
            KeyType = keyType;
            ValueType = valueType;
            ValueContainsNull = valueContainsNull;
        }

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitMapType(this);
        }
    }
}
