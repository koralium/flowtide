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

namespace FlowtideDotNet.Storage.FileCache
{
    internal enum SegmentCommandType
    {
        Read,
        Write
    }
    internal abstract class SegmentCommand
    {
        protected SegmentCommand(SegmentCommandType type)
        {
            Type = type;
        }

        public SegmentCommandType Type { get; }
    }

    internal class WriteSegmentCommand : SegmentCommand
    {
        public WriteSegmentCommand(long position, byte[] bytes, TaskCompletionSource taskCompletionSource) : base(SegmentCommandType.Write)
        {
            Position = position;
            Bytes = bytes;
            TaskCompletionSource = taskCompletionSource;
        }

        public byte[] Bytes { get; }
        public TaskCompletionSource TaskCompletionSource { get; }
        public long Position { get; }
    }

    internal class ReadSegmentCommand : SegmentCommand
    {
        public ReadSegmentCommand(long position, int length, TaskCompletionSource<byte[]> taskCompletionSource) : base(SegmentCommandType.Read)
        {
            Position = position;
            Length = length;
            TaskCompletionSource = taskCompletionSource;
        }

        public long Position { get; }
        public int Length { get; }
        public TaskCompletionSource<byte[]> TaskCompletionSource { get; }
    }
}
