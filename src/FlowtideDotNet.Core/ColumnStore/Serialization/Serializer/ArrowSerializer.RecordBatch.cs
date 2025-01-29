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

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
#pragma warning disable CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    internal ref partial struct ArrowSerializer
#pragma warning restore CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    {
        public int CreateRecordBatch(
            long length = 0,
            int nodesOffset = 0,
            int buffersOffset = 0,
            int compressionOffset = 0,
            int variadicCountsOffset = 0)
        {
            StartTable(5);
            AddLength(length);
            AddVariadicCounts(variadicCountsOffset);
            AddCompression(compressionOffset);
            AddBuffers(buffersOffset);
            AddNodes(nodesOffset);

            return EndTable();
        }

        void AddLength(long length) 
        { 
            AddLong(0, length, 0); 
        }

        void AddVariadicCounts(int variadicCountsOffset) 
        { 
            AddOffset(4, variadicCountsOffset, 0); 
        }

        void AddCompression(int compressionOffset) 
        { 
            AddOffset(3, compressionOffset, 0); 
        }

        void AddBuffers(int buffersOffset) 
        { 
            AddOffset(2, buffersOffset, 0); 
        }

        void AddNodes(int nodesOffset) 
        { 
            AddOffset(1, nodesOffset, 0); 
        }

        public void RecordBatchStartNodesVector(int numElems) 
        { 
            StartVector(16, numElems, 8); 
        }

        public void RecordBatchStartBuffersVector(int numElems) 
        { 
            StartVector(16, numElems, 8); 
        }

        public static void UpdateBufferLengthAndOffsetAtIndex(Span<byte> data, int buffersOffset, int index, long offset, long length)
        {
            var bufferOffset = buffersOffset + sizeof(int) + (index * 16);

            BinaryPrimitives.WriteInt64LittleEndian(data.Slice(bufferOffset), offset);
            BinaryPrimitives.WriteInt64LittleEndian(data.Slice(bufferOffset + sizeof(long)), length);
        }
    }
}
