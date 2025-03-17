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

using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using System.Buffers.Binary;

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
