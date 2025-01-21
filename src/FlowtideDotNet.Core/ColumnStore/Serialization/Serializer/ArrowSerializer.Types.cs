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

using Apache.Arrow.Types;
using FlexBuffers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.ExactNumberInfo;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal enum ArrowType : byte
    {
        NONE = 0,
        Null = 1,
        Int = 2,
        FloatingPoint = 3,
        Binary = 4,
        Utf8 = 5,
        Bool = 6,
        Decimal = 7,
        Date = 8,
        Time = 9,
        Timestamp = 10,
        Interval = 11,
        List = 12,
        Struct_ = 13,
        Union = 14,
        FixedSizeBinary = 15,
        FixedSizeList = 16,
        Map = 17,
        Duration = 18,
        LargeBinary = 19,
        LargeUtf8 = 20,
        LargeList = 21,
        RunEndEncoded = 22,
        BinaryView = 23,
        Utf8View = 24,
        ListView = 25,
        LargeListView = 26,
    };

    internal enum ArrowFloatingPrecision : short
    {
        HALF = 0,
        SINGLE = 1,
        DOUBLE = 2,
    };

    internal enum ArrowUnionMode : short
    {
        Sparse = 0,
        Dense = 1,
    };

    internal ref partial struct ArrowSerializer
    {
        public int AddUtf8Type()
        {
            StartTable(0);
            return EndTable();
        }

        public int AddInt64Type()
        {
            StartTable(2);
            AddBitWidth(64);
            AddIsSigned(true);
            return EndTable();
        }

        void AddBitWidth(int bitWidth)
        {
            AddInt(0, bitWidth, 0);
        }

        void AddIsSigned(bool isSigned)
        {
            AddBool(1, isSigned, false);
        }

        public int AddNullType()
        {
            StartTable(0);
            return EndTable();
        }

        public int AddDoubleType()
        {
            return CreateFloatingPointType(ArrowFloatingPrecision.DOUBLE);
        }

        public int CreateFloatingPointType(ArrowFloatingPrecision precision)
        {
            StartTable(1);
            FloatingPoinTypeAddPrecision(precision);
            return EndTable();
        }

        void FloatingPoinTypeAddPrecision(ArrowFloatingPrecision precision) 
        { 
            AddShort(0, (short)precision, 0);
        }

        public int AddBooleanType()
        {
            StartTable(0);
            return EndTable();
        }

        public int AddBinaryType()
        {
            StartTable(0);
            return EndTable();
        }

        public int AddListType()
        {
            StartTable(0);
            return EndTable();
        }

        public int AddUnionType(int typeIdsOffset, ArrowUnionMode mode)
        {
            StartTable(2);
            UnionTypeAddTypeIds(typeIdsOffset);
            UnionTypeAddMode(mode);
            return EndTable();
        }

        public int AddUnionType(Span<int> typeIds, ArrowUnionMode mode)
        {
            return AddUnionType(UnionCreateTypeIdsVector(typeIds), mode);
        }

        public int UnionCreateTypeIdsVector(Span<int> data) 
        {
            StartVector(4, data.Length, 4);
            for (int i = data.Length - 1; i >= 0; i--) 
            { 
                AddInt(data[i]); 
            } 
            return EndVector(); 
        }

        void UnionTypeAddTypeIds(int typeIdsOffset) 
        { 
            AddOffset(1, typeIdsOffset, 0); 
        }

        void UnionTypeAddMode(ArrowUnionMode mode) 
        { 
            AddShort(0, (short)mode, 0); 
        }

        public int AddMapType(bool keysSorted)
        {
            StartTable(1);
            MapTypeAddKeysSorted(keysSorted);
            return EndTable();
        }

        public int AddStructType()
        {
            StartTable(0);
            return EndTable();
        }

        void MapTypeAddKeysSorted(bool keysSorted) 
        { 
            AddBool(0, keysSorted, false); 
        }

        public int AddFixedSizeBinaryType(int byteWidth)
        {
            StartTable(1);
            FixedSizeBinaryTypeAddByteWidth(byteWidth);
            return EndTable();
        }

        void FixedSizeBinaryTypeAddByteWidth(int byteWidth) 
        { 
            AddInt(0, byteWidth, 0); 
        }
    }
}
