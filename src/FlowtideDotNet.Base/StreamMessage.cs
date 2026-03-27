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
    /// <summary>
    /// Base abstract class for data-carrying messages flowing through the stream.
    /// </summary>
    /// <remarks>
    /// Stream messages encapsulate actual data records (or batches of records) as opposed to control 
    /// events like watermarks or checkpoints.
    /// </remarks>
    public abstract class StreamMessage : IStreamEvent
    {
        /// <summary>
        /// Gets or internal sets the logical stream time associated with this message.
        /// </summary>
        public abstract long Time { get; internal set; }
    }

    /// <summary>
    /// Strongly-typed data message flowing through the stream.
    /// </summary>
    /// <typeparam name="T">The type of the underlying data payload.</typeparam>
    public class StreamMessage<T> : StreamMessage
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamMessage{T}"/> class.
        /// </summary>
        /// <param name="data">The payload data to be transported.</param>
        /// <param name="time">The logical stream time associated with this message.</param>
        public StreamMessage(T data, long time)
        {
            Data = data;
            Time = time;
        }

        /// <summary>
        /// Gets the strongly-typed payload data transported by this message.
        /// </summary>
        public T Data { get; }

        /// <summary>
        /// Gets or internal sets the current logical stream time associated with this message.
        /// </summary>
        public override long Time { get; internal set; }
    }
}
