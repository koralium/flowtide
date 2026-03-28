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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.Reservoir
{
    public class CompressionTests
    {
        [Fact]
        public void TestCompression()
        {
            

            byte[] data = new byte[] {
                1,
                2,
                3,
                4,
                5
            };

            ZstdCompression zstdCompression = new ZstdCompression(GlobalMemoryManager.Instance, 3, data.Length, 4);
            zstdCompression.Write(data);
            var result = zstdCompression.Complete();

            ZstdDecompression zstdDecompression = new ZstdDecompression(GlobalMemoryManager.Instance);
            var output = new byte[5];
            zstdDecompression.Read(new System.Buffers.ReadOnlySequence<byte>(result.memoryOwner.Memory.Slice(4, result.writtenLength)), output);
            Assert.Equal(data, output);
        }

        [Fact]
        public void TestCompressionLarge()
        {
            byte[] data = new byte[1024 * 128];

            for (int i = 0; i < data.Length; i++)
            {
                data[i] = (byte)(i % 256);
            }

            ZstdCompression zstdCompression = new ZstdCompression(GlobalMemoryManager.Instance, 3, data.Length, 0);
            zstdCompression.Write(data);
            var result = zstdCompression.Complete();

            ZstdDecompression zstdDecompression = new ZstdDecompression(GlobalMemoryManager.Instance);
            var output = new byte[data.Length];
            zstdDecompression.Read(new System.Buffers.ReadOnlySequence<byte>(result.memoryOwner.Memory.Slice(0, result.writtenLength)), output);
            Assert.Equal(data, output);
        }

        [Fact]
        public void TestDecompressMultiSegment()
        {
            byte[] data = new byte[1024 * 1024];

            for (int i = 0; i < data.Length; i++)
            {
                data[i] = (byte)(i % 256);
            }

            ZstdCompression zstdCompression = new ZstdCompression(GlobalMemoryManager.Instance, 3, data.Length, 0);
            zstdCompression.Write(data);
            var result = zstdCompression.Complete();

            List<BufferSegment> segments = new List<BufferSegment>();

            var resultMemory = result.memoryOwner.Memory.Slice(0, result.writtenLength);
            int dataDivide = resultMemory.Length / 16;

            var resultArr = resultMemory.ToArray();
            BufferSegment? head = default;
            BufferSegment? end = default;
            for (int i = 0; i < 16; i++)
            {
                int segmentSize = (i == 15) ? resultMemory.Length - dataDivide * 15 : dataDivide;
                byte[] segment = new byte[segmentSize];
                Array.Copy(resultArr, i * dataDivide, segment, 0, segmentSize);
                var seg = new BufferSegment(segment);
                if (head == null)
                {
                    head = seg;
                    end = seg;
                }
                else
                {
                    end!.SetNext(seg);
                    end = seg;
                    
                }
            }

            Assert.NotNull(head);
            Assert.NotNull(end);

            var readSeq = new ReadOnlySequence<byte>(head, 0, end, end.Length);


            ZstdDecompression zstdDecompression = new ZstdDecompression(GlobalMemoryManager.Instance);
            var output = new byte[data.Length];
            zstdDecompression.Read(readSeq, output);
            Assert.Equal(data, output);
        }
    }
}
