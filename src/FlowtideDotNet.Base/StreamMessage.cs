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

namespace FlowtideDotNet.Base
{
    public abstract class StreamMessage : IStreamEvent
    {
        public abstract long Time { get; internal set; }
    }

    public class StreamMessage<T> : StreamMessage
    {
        public StreamMessage(T data, long time)
        {
            Data = data;
            Time = time;
        }

        public T Data { get; }

        /// <summary>
        /// The current time
        /// </summary>
        public override long Time { get; internal set; }
    }
}
