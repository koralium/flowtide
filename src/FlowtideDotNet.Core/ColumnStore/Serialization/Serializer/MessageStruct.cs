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
        private readonly Span<byte> span;
        private readonly int position;

        public MessageStruct(Span<byte> span, int offset)
        {
            this.span = span;
            this.position = offset;
        }

        public long BodyLength 
        { 
            get 
            { 
                int o = __offset(10); 
                return o != 0 ? GetLong(o + position) : (long)0; 
            } 
        }

        public void SetBodyLength(long value)
        {
            int o = __offset(10);
            SetLong(o + position, value);
        }

        private void SetLong(int offset, long value)
        {
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(offset), value);
        }

        private long GetLong(int offset)
        {
            ReadOnlySpan<byte> span = this.span.Slice(offset);
            return BitConverter.ToInt64(span);
        }

        public static MessageStruct ReadMessage(ref readonly Span<byte> span)
        {
            var ipcMessage = BinaryPrimitives.ReadInt32LittleEndian(span);
            if (ipcMessage != -1)
            {
                throw new Exception("Not an IPC message");
            }
            var length = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4));
            return GetRootAsMessage(in span, 8);
        }

        public static MessageStruct GetRootAsMessage(ref readonly Span<byte> span, int position)
        {
            var offset = GetInt(in span, in position) + position;
            return new MessageStruct(span, offset);
        }

        public static int GetInt(ref readonly Span<byte> span, ref readonly int offset)
        {
            ReadOnlySpan<byte> readSpan = span.Slice(offset);
            return BinaryPrimitives.ReadInt32LittleEndian(readSpan);
        }

        public SchemaStruct Schema => GetSchema();

        public SchemaStruct GetSchema()
        {
            int o = __offset(8);
            return new SchemaStruct(span, __indirect(position + o));
        }

        public RecordBatchStruct RecordBatch()
        {
            int o = __offset(8);
            return new RecordBatchStruct(span, __indirect(position + o));
        }

        public int HeaderPosition()
        {
            int o = __offset(8);
            return __indirect(position + o);
        }

        public Span<byte> Span => span;


        private int __offset(int vtableOffset)
        {
            int vtable = position - GetInt(in span, in position);
            if (vtableOffset < GetShort(vtable))
            {
                return GetShort(vtable + vtableOffset);
            }
            else
            {
                return 0;
            }
        }

        private short GetShort(int position)
        {
            ReadOnlySpan<byte> span = this.span.Slice(position);
            return BinaryPrimitives.ReadInt16LittleEndian(span);
        }

        private int __indirect(int offset)
        {
            return offset + GetInt(in span, in offset);
        }
    }
}
