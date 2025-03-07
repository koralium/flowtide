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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class ArrayType : SchemaBaseType
    {
        public override SchemaType Type => SchemaType.Array;

        public SchemaBaseType? ElementType { get; set; }

        public bool ContainsNull { get; set; }

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitArrayType(this);
        }
    }
}
