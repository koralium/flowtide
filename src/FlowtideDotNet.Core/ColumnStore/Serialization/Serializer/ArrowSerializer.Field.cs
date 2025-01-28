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

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
#pragma warning disable CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    internal ref partial struct ArrowSerializer
#pragma warning restore CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    {
        public int CreateField(
            int nameOffset = 0,
            bool nullable = false,
            ArrowType type_type = 0,
            int typeOffset = 0,
            int dictionaryOffset = 0,
            int childrenOffset = 0,
            int custom_metadataOffset = 0)
        {
            StartTable(7);
            FieldAddCustomMetadata(custom_metadataOffset);
            FieldAddChildren(childrenOffset);
            FieldAddDictionary(dictionaryOffset);
            FieldAddType(typeOffset);
            FieldAddName(nameOffset);
            FieldAddTypeType(type_type);
            FieldAddNullable(nullable);
            return EndTable();
        }

        public int CreateChildrenVector(Span<int> data) 
        {
            StartVector(4, data.Length, 4);
            for (int i = data.Length - 1; i >= 0; i--)
            {
                AddOffset(data[i]);
            }
            return EndVector(); 
        }

        public int CreateCustomMetadataVector(Span<int> data)
        {
            StartVector(4, data.Length, 4);
            for (int i = data.Length - 1; i >= 0; i--)
            {
                AddOffset(data[i]);
            }
            return EndVector();
        }

        private void FieldAddCustomMetadata(int offset)
        {
            AddOffset(6, offset, 0);
        }

        void FieldAddChildren(int childrenOffset)
        {
            AddOffset(5, childrenOffset, 0);
        }

        void FieldAddDictionary(int dictionaryOffset)
        {
            AddOffset(4, dictionaryOffset, 0);
        }

        void FieldAddType(int typeOffset)
        {
            AddOffset(3, typeOffset, 0);
        }

        void FieldAddName(int nameOffset)
        {
            AddOffset(0, nameOffset, 0);
        }

        void FieldAddTypeType(ArrowType typeType)
        {
            AddByte(2, (byte)typeType, 0);
        }

        void FieldAddNullable(bool nullable)
        {
            AddBool(1, nullable, false);
        }
    }
}
