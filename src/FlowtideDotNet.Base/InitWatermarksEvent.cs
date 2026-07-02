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
    /// Represents an initialization event that broadcasts the names of watermarks across the stream.
    /// </summary>
    /// <remarks>
    /// This event implements <see cref="ILockingEvent"/> and is used to synchronize the watermark 
    /// expectations of multiple input vertices. By establishing these names early, downstream operators 
    /// can correctly track, compare, and wait for the appropriate watermarks before advancing their internal time.
    /// </remarks>
    internal class InitWatermarksEvent : ILockingEvent
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="InitWatermarksEvent"/> class with an empty set of watermark names.
        /// </summary>
        public InitWatermarksEvent()
        {
            WatermarkNames = ImmutableHashSet<string>.Empty;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InitWatermarksEvent"/> class with the specified watermark names.
        /// </summary>
        /// <param name="watermarkNames">The initial set of watermark names.</param>
        public InitWatermarksEvent(IReadOnlySet<string> watermarkNames)
        {
            WatermarkNames = watermarkNames;
        }

        /// <summary>
        /// Gets the read-only set of watermark names associated with this initialization event.
        /// </summary>
        public IReadOnlySet<string> WatermarkNames { get; }

        /// <summary>
        /// Creates a new <see cref="InitWatermarksEvent"/> by combining the existing watermark names 
        /// with a new set of watermark names.
        /// </summary>
        /// <param name="watermarkNames">The additional watermark names to include.</param>
        /// <returns>A new <see cref="InitWatermarksEvent"/> containing the union of both sets of names.</returns>
        public InitWatermarksEvent AddWatermarkNames(IReadOnlySet<string> watermarkNames)
        {
            HashSet<string> newSet = new HashSet<string>(WatermarkNames);

            foreach (var name in watermarkNames)
            {
                newSet.Add(name);
            }

            return new InitWatermarksEvent(newSet);
        }
    }
}
