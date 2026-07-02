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

using System.Collections.Immutable;

namespace FlowtideDotNet.Base
{
    /// <summary>
    /// Event that contains watermarks from the sources.
    /// Each source has unique watermarks to allow the egress to keep track of which offset from each source it is finished with.
    /// </summary>
    public class Watermark : IStreamEvent
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Watermark"/> class with a single watermark value, using the current UTC time as the start time.
        /// </summary>
        /// <param name="name">The unique name identifying the source of the watermark.</param>
        /// <param name="value">The temporal value or offset of the watermark.</param>
        public Watermark(string name, AbstractWatermarkValue value)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, AbstractWatermarkValue>();
            builder.Add(name, value);
            Watermarks = builder.ToImmutable();
            StartTime = DateTimeOffset.UtcNow;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Watermark"/> class with a single watermark value and a specified start time.
        /// </summary>
        /// <param name="name">The unique name identifying the source of the watermark.</param>
        /// <param name="value">The temporal value or offset of the watermark.</param>
        /// <param name="startTime">The time when this watermark started or was created.</param>
        public Watermark(string name, AbstractWatermarkValue value, DateTimeOffset startTime)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, AbstractWatermarkValue>();
            builder.Add(name, value);
            Watermarks = builder.ToImmutable();
            StartTime = startTime;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Watermark"/> class with a dictionary of watermarks, using the current UTC time as the start time.
        /// </summary>
        /// <param name="watermarks">An immutable dictionary representing multiple source watermarks.</param>
        public Watermark(IImmutableDictionary<string, AbstractWatermarkValue> watermarks)
        {
            Watermarks = watermarks;
            StartTime = DateTimeOffset.UtcNow;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Watermark"/> class with a dictionary of watermarks and a specified start time.
        /// </summary>
        /// <param name="watermarks">An immutable dictionary representing multiple source watermarks.</param>
        /// <param name="startTime">The time when this composite watermark started or was created.</param>
        public Watermark(IImmutableDictionary<string, AbstractWatermarkValue> watermarks, DateTimeOffset startTime)
        {
            Watermarks = watermarks;
            StartTime = startTime;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Watermark"/> class with a dictionary of watermarks, a start time, and the ID of the operator that sourced the data.
        /// </summary>
        /// <param name="watermarks">An immutable dictionary representing multiple source watermarks.</param>
        /// <param name="startTime">The time when this watermark started or was created.</param>
        /// <param name="sourceOperatorId">The identifier of the operator reporting this watermark.</param>
        public Watermark(IImmutableDictionary<string, AbstractWatermarkValue> watermarks, DateTimeOffset startTime, string? sourceOperatorId) : this(watermarks, startTime)
        {
            SourceOperatorId = sourceOperatorId;
        }

        /// <summary>
        /// Gets the immutable collection of watermark values mapped by their source name.
        /// </summary>
        public IImmutableDictionary<string, AbstractWatermarkValue> Watermarks { get; }

        /// <summary>
        /// Gets the starting timestamp representing when this watermark was initiated.
        /// </summary>
        public DateTimeOffset StartTime { get; }

        /// <summary>
        /// Gets the unique identifier for the source operator that generated or forwarded this watermark, if set.
        /// </summary>
        public string? SourceOperatorId { get; internal set; }

        /// <summary>
        /// Determines whether the specified object is equal to the current watermark.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns><c>true</c> if the specified object is equal to the current watermark; otherwise, <c>false</c>.</returns>
        public override bool Equals(object? obj)
        {
            return obj is Watermark watermark &&
                   EqualityComparer<IImmutableDictionary<string, AbstractWatermarkValue>>.Default.Equals(Watermarks, watermark.Watermarks);
        }

        /// <summary>
        /// Hash function for the watermark, combining the underlying dictionary elements.
        /// </summary>
        /// <returns>A hash code for the current object.</returns>
        public override int GetHashCode()
        {
            return HashCode.Combine(Watermarks);
        }
    }
}
