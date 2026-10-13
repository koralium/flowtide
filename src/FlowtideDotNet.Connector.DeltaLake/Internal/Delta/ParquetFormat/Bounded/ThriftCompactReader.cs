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

using System.Buffers.Binary;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded
{
    internal enum ThriftType : byte
    {
        Stop = 0,
        BoolTrue = 1,
        BoolFalse = 2,
        Byte = 3,
        I16 = 4,
        I32 = 5,
        I64 = 6,
        Double = 7,
        Binary = 8,
        List = 9,
        Set = 10,
        Map = 11,
        Struct = 12
    }

    /// <summary>
    /// Malformed or out-of-limit Thrift; the root is out of the envelope.
    /// </summary>
    internal sealed class ThriftFormatException : Exception
    {
        public ThriftFormatException(string message) : base(message)
        {
        }
    }

    /// <summary>
    /// The limits and native metadata totals shared by every read of one footer.
    /// </summary>
    internal sealed class ThriftBudget
    {
        public const int DefaultMaxDepth = 32;
        public const long DefaultMaxEntries = 1_048_576;

        public ThriftBudget(long maxString, int maxDepth = DefaultMaxDepth, long maxEntries = DefaultMaxEntries)
        {
            MaxString = maxString;
            MaxDepth = maxDepth;
            MaxEntries = maxEntries;
        }

        public long MaxString { get; }

        public int MaxDepth { get; }

        public long MaxEntries { get; }

        public long Entries { get; set; }

        public long StringBytes { get; set; }

        public long ListEntries { get; set; }
    }

    /// <summary>
    /// The Thrift compact protocol over a span, allocating nothing; skipped fields are walked so their sizes are counted.
    /// </summary>
    internal ref struct ThriftCompactReader
    {
        private readonly ReadOnlySpan<byte> _data;
        private readonly ThriftBudget _budget;
        private int _position;
        private int _depth;

        public ThriftCompactReader(ReadOnlySpan<byte> data, ThriftBudget budget)
        {
            _data = data;
            _budget = budget;
            _position = 0;
            _depth = 0;
        }

        public readonly int Position => _position;

        private byte ReadByte()
        {
            if (_position >= _data.Length)
            {
                throw new ThriftFormatException("Thrift data ends early");
            }
            return _data[_position++];
        }

        private ulong ReadVarint()
        {
            ulong result = 0;
            for (int shift = 0; shift < 64; shift += 7)
            {
                var b = ReadByte();
                result |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                {
                    return result;
                }
            }
            throw new ThriftFormatException("A Thrift varint is too long");
        }

        private static long Unzigzag(ulong value) => (long)(value >> 1) ^ -(long)(value & 1);

        public void EnterStruct()
        {
            if (++_depth > _budget.MaxDepth)
            {
                throw new ThriftFormatException($"Thrift nesting is deeper than {_budget.MaxDepth}");
            }
        }

        public void LeaveStruct()
        {
            _depth--;
        }

        /// <summary>
        /// The next field of the current struct, false at its end; booleans carry their value in the type, and a repeated id is refused.
        /// </summary>
        public bool ReadField(ref short lastId, ref ulong seen, out short id, out ThriftType type)
        {
            var header = ReadByte();
            type = (ThriftType)(header & 0x0F);
            if (type == ThriftType.Stop)
            {
                id = 0;
                return false;
            }
            if (type > ThriftType.Struct)
            {
                throw new ThriftFormatException($"Invalid Thrift field type {(int)type}");
            }
            var delta = header >> 4;
            if (delta == 0)
            {
                var raw = Unzigzag(ReadVarint());
                if (raw < short.MinValue || raw > short.MaxValue)
                {
                    throw new ThriftFormatException("A Thrift field id is out of range");
                }
                id = (short)raw;
            }
            else
            {
                id = (short)(lastId + delta);
            }
            lastId = id;
            // The native reader keeps the last occurrence, so a repeat would make our model differ from it
            if (id >= 0 && id < 64)
            {
                var bit = 1UL << id;
                if ((seen & bit) != 0)
                {
                    throw new ThriftFormatException($"Thrift field {id} is repeated");
                }
                seen |= bit;
            }
            return true;
        }

        public int ReadI32(ThriftType type)
        {
            // The native reader skips a field of another wire type, so only the exact type is read
            if (type != ThriftType.I32)
            {
                throw new ThriftFormatException($"Expected an i32, found Thrift type {type}");
            }
            var value = Unzigzag(ReadVarint());
            if (value < int.MinValue || value > int.MaxValue)
            {
                throw new ThriftFormatException("A Thrift i32 is out of range");
            }
            return (int)value;
        }

        public long ReadI64(ThriftType type)
        {
            if (type != ThriftType.I64)
            {
                throw new ThriftFormatException($"Expected an i64, found Thrift type {type}");
            }
            return Unzigzag(ReadVarint());
        }

        public static bool ReadBool(ThriftType type)
        {
            return type switch
            {
                ThriftType.BoolTrue => true,
                ThriftType.BoolFalse => false,
                _ => throw new ThriftFormatException($"Expected a boolean, found Thrift type {type}")
            };
        }

        public ReadOnlySpan<byte> ReadBinary(ThriftType type)
        {
            if (type != ThriftType.Binary)
            {
                throw new ThriftFormatException($"Expected a string, found Thrift type {type}");
            }
            var length = ReadVarint();
            if (length > (ulong)_budget.MaxString || length > (ulong)(_data.Length - _position))
            {
                throw new ThriftFormatException($"A Thrift string of {length} bytes");
            }
            var value = _data.Slice(_position, (int)length);
            _position += (int)length;
            _budget.StringBytes += value.Length;
            return value;
        }

        /// <summary>
        /// A list header: its element type and size, counted against the entry budget; the native reader skips a set where a list is declared.
        /// </summary>
        public int ReadListHeader(ThriftType type, out ThriftType elementType)
        {
            if (type != ThriftType.List)
            {
                throw new ThriftFormatException($"Expected a list, found Thrift type {type}");
            }
            return ReadContainerHeader(out elementType);
        }

        private int ReadContainerHeader(out ThriftType elementType)
        {
            var header = ReadByte();
            elementType = (ThriftType)(header & 0x0F);
            long size = header >> 4;
            if (size == 15)
            {
                var raw = ReadVarint();
                if (raw > int.MaxValue)
                {
                    throw new ThriftFormatException("A Thrift list size is out of range");
                }
                size = (long)raw;
            }
            Count(size);
            _budget.ListEntries += size;
            return (int)size;
        }

        private void Count(long entries)
        {
            _budget.Entries += entries;
            if (_budget.Entries > _budget.MaxEntries)
            {
                throw new ThriftFormatException($"More than {_budget.MaxEntries} Thrift container entries");
            }
            // Every entry takes at least one byte, so a size past the remaining data is a lie
            if (entries > _data.Length - _position)
            {
                throw new ThriftFormatException($"A Thrift container claims {entries} entries in {_data.Length - _position} bytes");
            }
        }

        // Elements inside lists use the element type; booleans are one byte each there
        public bool ReadListBool()
        {
            return ReadByte() switch
            {
                1 => true,
                0 or 2 => false,
                var other => throw new ThriftFormatException($"Invalid Thrift list boolean {other}")
            };
        }

        public void Skip(ThriftType type)
        {
            switch (type)
            {
                case ThriftType.BoolTrue:
                case ThriftType.BoolFalse:
                    return;
                case ThriftType.Byte:
                    ReadByte();
                    return;
                case ThriftType.I16:
                case ThriftType.I32:
                case ThriftType.I64:
                    ReadVarint();
                    return;
                case ThriftType.Double:
                    if (_data.Length - _position < 8)
                    {
                        throw new ThriftFormatException("Thrift data ends early");
                    }
                    _position += 8;
                    return;
                case ThriftType.Binary:
                    ReadBinary(type);
                    return;
                case ThriftType.List:
                case ThriftType.Set:
                    {
                        var count = ReadContainerHeader(out var element);
                        EnterStruct();
                        for (int i = 0; i < count; i++)
                        {
                            SkipElement(element);
                        }
                        LeaveStruct();
                        return;
                    }
                case ThriftType.Map:
                    {
                        var raw = ReadVarint();
                        if (raw > int.MaxValue)
                        {
                            throw new ThriftFormatException("A Thrift map size is out of range");
                        }
                        var count = (long)raw;
                        if (count == 0)
                        {
                            return;
                        }
                        Count(count);
                        var types = ReadByte();
                        EnterStruct();
                        for (long i = 0; i < count; i++)
                        {
                            SkipElement((ThriftType)(types >> 4));
                            SkipElement((ThriftType)(types & 0x0F));
                        }
                        LeaveStruct();
                        return;
                    }
                case ThriftType.Struct:
                    {
                        EnterStruct();
                        short last = 0;
                        ulong seen = 0;
                        while (ReadField(ref last, ref seen, out _, out var fieldType))
                        {
                            Skip(fieldType);
                        }
                        LeaveStruct();
                        return;
                    }
                default:
                    throw new ThriftFormatException($"Invalid Thrift type {(int)type}");
            }
        }

        public void SkipElement(ThriftType element)
        {
            if (element == ThriftType.BoolTrue || element == ThriftType.BoolFalse)
            {
                ReadListBool();
                return;
            }
            Skip(element);
        }

        public static int ReadInt32LittleEndian(ReadOnlySpan<byte> data) => BinaryPrimitives.ReadInt32LittleEndian(data);
    }
}
