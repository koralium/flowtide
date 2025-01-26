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

using Apache.Arrow;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal ref partial struct ArrowSerializer
    {
        private Span<byte> m_destination;
        private Span<int> m_vtable;
        private Span<int> m_vtables;

        private int m_minAlign;
        private int m_space;
        private int m_maxSize;
        private int m_vectorNumElems;

        private int m_vtableSize;
        private int m_objectStart;
        private int m_numVtables = 0;

        private int m_position;

        public ArrowSerializer(Span<byte> memory, Span<int> vtable, Span<int> vtables)
        {
            m_destination = memory;
            m_vtable = vtable;
            m_vtables = vtables;
            m_maxSize = m_destination.Length;
            m_space = m_maxSize;
        }

        public void SetSpacePosition(int space)
        {
            m_space = space;
        }

        public void Reset(Span<byte> memory, Span<int> vtable, Span<int> vtables, int space)
        {
            m_destination = memory;
            m_vtable = vtable;
            m_vtables = vtables;
            m_maxSize = m_destination.Length;
            m_space = space;
        }

        private int Offset { get { return m_maxSize - m_space; } }

        public int Position { get { return m_position; } }

        public Span<byte> CopyToStart(int padding)
        {
            m_destination.Slice(m_space).CopyTo(m_destination);
            var outputSpan = m_destination.Slice(0, m_maxSize - m_space + padding);
            m_position += outputSpan.Length + padding;
            m_destination = m_destination.Slice(Offset + padding);
            m_maxSize = m_destination.Length;
            m_space = m_maxSize;
            return outputSpan;
        }

        public int WriteMessageLengthAndPadding()
        {

            var messagePaddingLength = CalculatePadding(8 + Offset);

            WriteIpcMessageLength(Offset + messagePaddingLength);
            return messagePaddingLength;
        }

        private void WriteIpcMessageLength(int length)
        {
            var startOffset = m_space - sizeof(int) * 2;
            BinaryPrimitives.WriteInt32LittleEndian(
                        m_destination.Slice(startOffset), -1);
            BinaryPrimitives.WriteInt32LittleEndian(m_destination.Slice(startOffset + sizeof(int)), length);
            m_space -= sizeof(int) * 2;
        }

        private int CalculatePadding(long offset, int alignment = 8)
        {
            long result = BitUtility.RoundUpToMultiplePowerOfTwo(offset, alignment) - offset;
            checked
            {
                return (int)result;
            }
        }

        public int CreateEmptyString()
        {
            AddByte(0);
            StartVector(1, 0, 1);
            return EndVector();
        }

        public int CreateString(string value)
        {
            AddByte(0);
            var utf8StringLen = Encoding.UTF8.GetByteCount(value);
            StartVector(1, utf8StringLen, 1);
            PutStringUTF8(m_space -= utf8StringLen, value);
            return EndVector();
        }

        public int CreateStringUtf8(ReadOnlySpan<byte> value)
        {
            AddByte(0);
            StartVector(1, value.Length, 1);
            value.CopyTo(m_destination.Slice(m_space -= value.Length));
            return EndVector();
        }

        public void AddByte(byte x) { Prep(sizeof(byte), 0); PutByte(x); }

        private void Prep(int size, int additionalBytes)
        {
            // Track the biggest thing we've ever aligned to.
            if (size > m_minAlign)
                m_minAlign = size;
            // Find the amount of alignment needed such that `size` is properly
            // aligned after `additional_bytes`
            var alignSize =
                ((~((int)m_maxSize - m_space + additionalBytes)) + 1) &
                (size - 1);

            if (alignSize > 0)
                Pad(alignSize);
        }

        public void Pad(int size)
        {
            m_space -= size;
        }

        void PutByte(int offset, byte value)
        {
            m_destination[offset] = value;
        }

        public void PutByte(byte x)
        {
            m_destination[m_space -= sizeof(byte)] = x;
        }

        public void StartVector(int elemSize, int count, int alignment)
        {
            m_vectorNumElems = count;
            Prep(sizeof(int), elemSize * count);
            Prep(alignment, elemSize * count); // Just in case alignment > int.
        }

        public unsafe void PutStringUTF8(int offset, string value)
        {
            fixed (char* s = value)
            {
                fixed (byte* buffer = &MemoryMarshal.GetReference(m_destination.Slice(0)))
                {
                    Encoding.UTF8.GetBytes(s, value.Length, buffer + offset, m_maxSize - offset);
                }
            }
        }

        public int EndVector()
        {
            PutInt(m_vectorNumElems);
            return Offset;
        }

        public void PutInt(int x)
        {
            m_space -= sizeof(int);
            
            Span<byte> span = m_destination.Slice(m_space);
            BinaryPrimitives.WriteInt32LittleEndian(span, x);
        }

        private void PutInt(int offset, int x)
        {
            Span<byte> span = m_destination.Slice(offset);
            BinaryPrimitives.WriteInt32LittleEndian(span, x);
        }

        public void PutShort(short x)
        {
            m_space -= sizeof(short);

            Span<byte> span = m_destination.Slice(m_space);
            BinaryPrimitives.WriteUInt16LittleEndian(span, (ushort)x);
        }

        private void StartTable(int numfields)
        {
            if (m_vtable.Length < numfields)
                throw new Exception("Vtable sizee was too small");

            m_vtableSize = numfields;
            m_objectStart = Offset;
        }

        private void AddInt(int value)
        {
            Prep(sizeof(int), 0);
            PutInt(value);
        }


        void AddInt(int o, int x, int d) 
        { 
            if (x != d) 
            { 
                AddInt(x); 
                Slot(o); 
            } 
        }

        void AddOffset(int o, int x, int d) 
        { 
            if (x != d) 
            { 
                AddOffset(x); 
                Slot(o); 
            } 
        }

        void AddByte(int o, byte x, byte d) 
        { 
            if (x != d) 
            { 
                AddByte(x); 
                Slot(o); 
            } 
        }

        void AddShort(int o, short x, int d) 
        { 
            if (x != d) 
            { 
                AddShort(x); 
                Slot(o); 
            } 
        }

        void AddLong(int o, long x, long d) 
        { 
            if (x != d) 
            { 
                AddLong(x); 
                Slot(o); 
            } 
        }

        void AddLong(long x) 
        { 
            Prep(sizeof(long), 0); 
            PutLong(x); 
        }

        void PutLong(long x)
        {
            PutLong(m_space -= sizeof(long), x);
        }

        void PutLong(int offset, long value)
        {
            Span<byte> span = m_destination.Slice(offset);
            BinaryPrimitives.WriteInt64LittleEndian(span, value);
        }

        void PutBool(bool x)
        {
            PutByte(m_space -= sizeof(byte), (byte)(x ? 1 : 0));
        }

        void AddBool(bool x) 
        { 
            Prep(sizeof(byte), 0); 
            PutBool(x); 
        }

        void AddBool(int o, bool x, bool d) 
        { 
            if (x != d) 
            { 
                AddBool(x); 
                Slot(o); 
            } 
        }

        void AddOffset(int off)
        {
            Prep(sizeof(int), 0);  // Ensure alignment is already done.
            if (off > Offset)
                throw new ArgumentException();

            if (off != 0)
                off = Offset - off + sizeof(int);
            PutInt(off);
        }

        void Slot(int voffset)
        {
            if (voffset >= m_vtableSize)
                throw new IndexOutOfRangeException("Flatbuffers: invalid voffset");

            m_vtable[voffset] = Offset;
        }

        private void AddShort(short value)
        {
            Prep(sizeof(short), 0);
            PutShort(value);
        }

        private short GetShort(int offset)
        {
            ReadOnlySpan<byte> span = m_destination.Slice(offset);
            return BinaryPrimitives.ReadInt16LittleEndian(span);
        }

        private ushort GetUShort(int offset)
        {
            ReadOnlySpan<byte> span = m_destination.Slice(offset);
            return BinaryPrimitives.ReadUInt16LittleEndian(span);
        }

        public int EndTable()
        {
            AddInt(0);
            var vtableloc = Offset;
            int i = m_vtableSize - 1;

            // Trim trailing zeroes.
            for (; i >= 0 && m_vtable[i] == 0; i--) { }

            int trimmedSize = i + 1;

            for (; i >= 0; i--)
            {
                // Offset relative to the start of the table.
                short off = (short)(m_vtable[i] != 0
                                        ? vtableloc - m_vtable[i]
                                        : 0);
                AddShort(off);

                // clear out written entry
                m_vtable[i] = 0;
            }

            const int standardFields = 2; // The fields below:
            AddShort((short)(vtableloc - m_objectStart));
            AddShort((short)((trimmedSize + standardFields) *
                             sizeof(short)));

            int existingVtable = 0;
            for (i = 0; i < m_numVtables; i++)
            {
                int vt1 = m_maxSize - m_vtables[i];
                int vt2 = m_space;
                short len = GetShort(vt1);
                if (len == GetShort(vt2))
                {
                    for (int j = sizeof(short); j < len; j += sizeof(short))
                    {
                        if (GetShort(vt1 + j) != GetShort(vt2 + j))
                        {
                            goto endLoop;
                        }
                    }
                    existingVtable = m_vtables[i];
                    break;
                }

            endLoop: { }
            }

            if (existingVtable != 0)
            {
                // Found a match:
                // Remove the current vtable.
                m_space = m_maxSize - vtableloc;
                // Point table to existing vtable.
                PutInt(m_space, existingVtable - vtableloc);
            }
            else
            {
                // No match:
                // Add the location of the current vtable to the list of
                // vtables.
                if (m_numVtables == m_vtables.Length)
                {
                    throw new Exception("Vtables array was too small");
                };
                m_vtables[m_numVtables++] = Offset;
                // Point table to current vtable.
                PutInt(m_maxSize - vtableloc, Offset - vtableloc);
            }

            m_vtableSize = -1;
            return vtableloc;
        }

        public int Finish(int rootTable)
        {
            return Finish(rootTable, false);
        }

        int Finish(int rootTable, bool sizePrefix)
        {
            Prep(m_minAlign, sizeof(int) + (sizePrefix ? sizeof(int) : 0));
            AddOffset(rootTable);
            if (sizePrefix)
            {
                AddInt(m_maxSize - m_space);
            }
            return m_space;
        }

        private static int CalculatePaddedBufferLength(int length)
        {
            long result = BitUtility.RoundUpToMultiplePowerOfTwo(length, 64);
            checked
            {
                return (int)result;
            }
        }
    }
}
