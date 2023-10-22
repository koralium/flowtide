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

using FlowtideDotNet.Base.dataflow;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Tests
{
    public class GuaranteedBroadcastBlockTests
    {
        [Fact]
        public async Task TestReplication()
        {
            var block = new GuaranteedBroadcastBlock<int>(new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = 10
            });

            var inputBuffer = new BufferBlock<int>(new DataflowBlockOptions()
            {
                BoundedCapacity = 10
            });
            var left = new BufferBlock<int>(new DataflowBlockOptions()
            {
                BoundedCapacity = 10
            });
            var right = new BufferBlock<int>(new DataflowBlockOptions()
            {
                BoundedCapacity = 10
            });

            inputBuffer.LinkTo(block);
            block.LinkTo(left);
            block.LinkTo(right);

            await inputBuffer.SendAsync(1);

            var leftVal = await left.ReceiveAsync();
            var rightVal = await right.ReceiveAsync();

            Assert.Equal(1, leftVal);
            Assert.Equal(1, rightVal);
        }

        [Fact]
        public async Task TestOneBufferFull()
        {
            var block = new GuaranteedBroadcastBlock<int>(new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = 10
            });

            var inputBuffer = new BufferBlock<int>(new DataflowBlockOptions()
            {
                BoundedCapacity = 10
            });
            var left = new BufferBlock<int>(new DataflowBlockOptions()
            {
                BoundedCapacity = 10
            });
            var right = new BufferBlock<int>(new DataflowBlockOptions()
            {
                BoundedCapacity = 10
            });

            for (int i = 0; i < 10; i++)
            {
                await right.SendAsync(i);
            }

            inputBuffer.LinkTo(block);
            block.LinkTo(left);
            block.LinkTo(right);

            await inputBuffer.SendAsync(1);

            var leftVal = await left.ReceiveAsync();

            for (int i = 0; i < 10; i++)
            {
                await right.ReceiveAsync();
            }

            var rightVal = await right.ReceiveAsync();

            Assert.Equal(1, leftVal);
            Assert.Equal(1, rightVal);
        }

        [Fact]
        public async Task TestDuplicateLinkSameTarget()
        {
            var block = new GuaranteedBroadcastBlock<int>(new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = 10
            });

            var inputBuffer = new BufferBlock<int>(new DataflowBlockOptions()
            {
                BoundedCapacity = 10
            });
            var output = new BufferBlock<int>(new DataflowBlockOptions()
            {
                BoundedCapacity = 10
            });

            inputBuffer.LinkTo(block);
            block.LinkTo(output);
            block.LinkTo(output);

            await inputBuffer.SendAsync(1);
            await inputBuffer.SendAsync(2);

            var first = await output.ReceiveAsync();
            var second = await output.ReceiveAsync();

            Assert.Equal(1, first);
            Assert.Equal(2, second);
        }
    }
}