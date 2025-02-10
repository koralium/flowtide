using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal enum MessageHeader : byte
    {
        NONE = 0,
        Schema = 1,
        DictionaryBatch = 2,
        RecordBatch = 3,
        Tensor = 4,
        SparseTensor = 5,
    };

    internal ref struct MessageStruct
    {
        private readonly ReadOnlySpan<byte> span;
        private readonly int position;

        public MessageStruct(ReadOnlySpan<byte> span, int offset)
        {
            this.span = span;
            this.position = offset;
        }

        public long BodyLength 
        { 
            get 
            { 
                int o = ReadUtils.__offset(in span, in position, 10); 
                return o != 0 ? ReadUtils.GetLong(in span, o + position) : (long)0; 
            } 
        }

        public long BodyLengthIndex
        {
            get
            {
                int o = ReadUtils.__offset(in span, in position, 10);
                return o + position;
            }
        }

        public static MessageStruct ReadMessage(ref readonly ReadOnlySpan<byte> span)
        {
            var ipcMessage = BinaryPrimitives.ReadInt32LittleEndian(span);
            if (ipcMessage != -1)
            {
                throw new Exception("Not an IPC message");
            }
            var length = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4));
            return GetRootAsMessage(in span, 8);
        }

        public static MessageStruct GetRootAsMessage(ref readonly ReadOnlySpan<byte> span, int position)
        {
            var offset = ReadUtils.GetInt(in span, position) + position;
            return new MessageStruct(span, offset);
        }

        public SchemaStruct Schema => GetSchema();

        public SchemaStruct GetSchema()
        {
            int o = ReadUtils.__offset(in span, in position, 8);
            return new SchemaStruct(span, ReadUtils.__indirect(in span, position + o));
        }

        public RecordBatchStruct RecordBatch()
        {
            int o = ReadUtils.__offset(in span, in position, 8);
            return new RecordBatchStruct(span, ReadUtils.__indirect(in span, position + o));
        }

        public int HeaderPosition()
        {
            int o = ReadUtils.__offset(in span, in position, 8);
            return ReadUtils.__indirect(in span, position + o);
        }

        public MessageHeader HeaderType => GetHeaderType();

        public MessageHeader GetHeaderType()
        {
            int o = ReadUtils.__offset(in span, in position, 6);
            return (MessageHeader)ReadUtils.Get(in span, o + position);
        }

        public ReadOnlySpan<byte> Span => span;
    }
}
