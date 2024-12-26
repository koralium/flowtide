using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
#pragma warning disable CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    internal ref partial struct ArrowSerializer
#pragma warning restore CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    {
        public int CreateMessage(
            short version = 0,
            MessageHeader headerType = 0,
            int headerOffset = 0,
            int bodyLength = 0,
            int custom_metadataOffset = 0)
        {
            StartTable(5);
            AddBodyLength(bodyLength);
            AddCustomMetadata(custom_metadataOffset);
            AddHeader(headerOffset);
            AddVersion(version);
            AddHeaderType(headerType);
            return EndTable();
        }

        void AddCustomMetadata(int customMetadataOffset) 
        { 
            AddOffset(4, customMetadataOffset, 0); 
        }

        void AddBodyLength(long bodyLength) 
        { 
            AddLong(3, bodyLength, 0); 
        }

        void AddHeader(int headerOffset) 
        { 
            AddOffset(2, headerOffset, 0); 
        }

        void AddVersion(short version) 
        { 
            AddShort(0, version, 0); 
        }

        void AddHeaderType(MessageHeader headerType) 
        { 
            AddByte(1, (byte)headerType, 0); 
        }
    }
}
