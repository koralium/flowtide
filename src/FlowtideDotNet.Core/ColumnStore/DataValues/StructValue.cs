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

using FlowtideDotNet.Core.Flexbuffer;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace FlowtideDotNet.Core.ColumnStore.DataValues
{
    public struct StructHeader : IEquatable<StructHeader>, IComparable<StructHeader>, IReadOnlyList<string>
    {
        internal int[] offsets;
        internal byte[] utf8Bytes;

        public int Count => offsets.Length - 1;

        public string this[int index] => throw new NotImplementedException();

        public StructHeader(int[] offsets, byte[] utf8bytes)
        {
            this.offsets = offsets;
            this.utf8Bytes = utf8bytes;
        }

        public static StructHeader Create(params string[] columnNames)
        {
            int byteCount = 0;
            for (int i = 0; i < columnNames.Length; i++)
            {
                byteCount += Encoding.UTF8.GetByteCount(columnNames[i]);
            }
            byte[] utf8bytes = new byte[byteCount];
            int[] offsets = new int[columnNames.Length + 1];

            int offset = 0;
            for (int i = 0; i < columnNames.Length; i++)
            {
                
                int byteCountForColumn = Encoding.UTF8.GetBytes(columnNames[i], 0, columnNames[i].Length, utf8bytes, offset);
                offset += byteCountForColumn;
                offsets[i + 1] = offset;
            }

            return new StructHeader(offsets, utf8bytes);
        }

        public Span<byte> GetColumnNameUtf8(int index)
        {
            if (index < 0 || index >= offsets.Length - 1)
            {
                throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} is out of range for StructHeader with {Count} columns.");
            }

            int start = offsets[index];
            int length = offsets[index + 1] - start;
            return utf8Bytes.AsSpan(start, length);
        }

        public int FindIndex(string columnName)
        {
            var str = new FlxString(Encoding.UTF8.GetBytes(columnName));
            return FindIndex(str);
        }

        public int FindIndex(FlxString str)
        {
            for (int i = 0; i < Count; i++)
            {
                var columnNameBytes = new FlxString(GetColumnNameUtf8(i));
                if (FlxString.CompareIgnoreCase(columnNameBytes, str) == 0)
                {
                    return i;
                }
            }
            return -1;
        }

        public string GetColumnName(int index)
        {
            return Encoding.UTF8.GetString(GetColumnNameUtf8(index));
        }

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            return obj is StructValue other &&
                Equals(other);
        }

        public bool Equals(StructHeader other)
        {
            if (offsets.Length != other.offsets.Length)
            {
                return false;
            }
            for (int i = 0; i < offsets.Length; i++)
            {
                if (offsets[i] != other.offsets[i])
                {
                    return false;
                }
            }
            if (utf8Bytes.Length != other.utf8Bytes.Length)
            {
                return false;
            }
            return utf8Bytes.AsSpan().SequenceEqual(other.utf8Bytes);
        }

        public override int GetHashCode()
        {
            HashCode hashCode = new HashCode();
            hashCode.AddBytes(utf8Bytes);
            return hashCode.ToHashCode();
        }

        public int CompareTo(StructHeader other)
        {
            var offsetCompare = offsets.AsSpan().SequenceCompareTo(other.offsets.AsSpan());

            if (offsetCompare != 0)
            {
                return offsetCompare;
            }

            var utf8Compare = utf8Bytes.AsSpan().SequenceCompareTo(other.utf8Bytes.AsSpan());
            return utf8Compare;
        }

        public IEnumerator<string> GetEnumerator()
        {
            for (int i = 0; i < Count; i++)
            {
                yield return GetColumnName(i);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    } 
    public struct StructValue : IStructValue
    {
        internal StructHeader _header;
        internal readonly IDataValue[] _columnValues;

        public StructHeader Header => _header;

        public ArrowTypeId Type => ArrowTypeId.Struct;

        public long AsLong => throw new NotImplementedException();

        public FlxString AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public ReadOnlySpan<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public IStructValue AsStruct => this;

        public StructValue(StructHeader header, params IDataValue[] columnValues)
        {
            _header = header;
            _columnValues = columnValues;

            if (_header.Count != columnValues.Length)
            {
                throw new ArgumentException($"StructValue header count {_header.Count} does not match column values count {columnValues.Length}");
            }
        }

        public IDataValue GetAt(in int index)
        {
            return _columnValues[index];
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._structValue = this;
            container._type = ArrowTypeId.Struct;
        }

        public void Accept(in DataValueVisitor visitor)
        {
            visitor.VisitStructValue(ref this);
        }
    }
}
