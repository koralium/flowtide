using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal ref partial struct ArrowSerializer
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
