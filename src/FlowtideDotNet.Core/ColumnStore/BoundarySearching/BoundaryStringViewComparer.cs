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

using FlowtideDotNet.Core.ColumnStore.Utils;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.ColumnStore.BoundarySearching
{
    internal unsafe struct BoundaryStringViewComparer : IBoundaryComparer
    {
        public BinaryViewList.ArrowBinaryView* TreeViews;
        public byte* TreeData;

        public BinaryViewList.ArrowBinaryView* InputViews;
        public byte* InputData;

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public int CompareTreeToInput(int treeIndex, int inputIndex)
        {
            return CompareViews(
                TreeViews + treeIndex, TreeData,
                InputViews + inputIndex, InputData);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static int CompareViews(
            BinaryViewList.ArrowBinaryView* viewX, byte* dataX,
            BinaryViewList.ArrowBinaryView* viewY, byte* dataY)
        {
            if (viewX->PrefixInt != viewY->PrefixInt)
            {
                uint valX = BitConverter.IsLittleEndian ? System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(viewX->PrefixInt) : viewX->PrefixInt;
                uint valY = BitConverter.IsLittleEndian ? System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(viewY->PrefixInt) : viewY->PrefixInt;
                return valX.CompareTo(valY);
            }

            ReadOnlySpan<byte> spanX;
            if (viewX->Length <= 12)
            {
                spanX = new ReadOnlySpan<byte>((byte*)(viewX) + 4, viewX->Length);
            }
            else
            {
                spanX = new ReadOnlySpan<byte>(dataX + viewX->Offset, viewX->Length);
            }

            ReadOnlySpan<byte> spanY;
            if (viewY->Length <= 12)
            {
                spanY = new ReadOnlySpan<byte>((byte*)(viewY) + 4, viewY->Length);
            }
            else
            {
                spanY = new ReadOnlySpan<byte>(dataY + viewY->Offset, viewY->Length);
            }

            return spanX.SequenceCompareTo(spanY);
        }

        public int CompareInputToInput(int inputIndexA, int inputIndexB)
        {
            return CompareViews(
            InputViews + inputIndexA, InputData,
            InputViews + inputIndexB, InputData);
        }
    }
}
