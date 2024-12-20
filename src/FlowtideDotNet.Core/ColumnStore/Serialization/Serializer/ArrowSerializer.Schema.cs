using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal ref partial struct ArrowSerializer
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
