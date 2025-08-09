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
        public int CreateSchema(
            short endianess = 0,
            int fieldsOffset = 0,
            int custom_metadataOffset = 0,
            int featuresOffset = 0)
        {
            StartTable(4);
            SchemaAddFeatures(featuresOffset);
            SchemaAddCustomMetadata(custom_metadataOffset);
            SchemaAddFields(fieldsOffset);
            SchemaAddEndianness(endianess);
            return EndTable();
        }

        void SchemaAddEndianness(short endianness)
        {
            AddShort(0, (short)endianness, 0);
        }

        void SchemaAddFields(int fieldsOffset)
        {
            AddOffset(1, fieldsOffset, 0);
        }

        void SchemaAddCustomMetadata(int customMetadataOffset)
        {
            AddOffset(2, customMetadataOffset, 0);
        }

        void SchemaAddFeatures(int featuresOffset)
        {
            AddOffset(3, featuresOffset, 0);
        }

        public int SchemaCreateFieldsVector(Span<int> data)
        {
            StartVector(4, data.Length, 4);
            for (int i = data.Length - 1; i >= 0; i--)
            {
                AddOffset(data[i]);
            }
            return EndVector();
        }
    }
}
