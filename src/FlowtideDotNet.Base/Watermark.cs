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
        public Watermark(string name, long offset)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, long>();
            builder.Add(name, offset);
            Watermarks = builder.ToImmutable();
            StartTime = DateTimeOffset.UtcNow;
        }

        public Watermark(string name, long offset, DateTimeOffset startTime)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, long>();
            builder.Add(name, offset);
            Watermarks = builder.ToImmutable();
            StartTime = startTime;
        }

        public Watermark(IImmutableDictionary<string, long> watermarks)
        {
            Watermarks = watermarks;
            StartTime = DateTimeOffset.UtcNow;
        }

        public Watermark(IImmutableDictionary<string, long> watermarks, DateTimeOffset startTime)
        {
            Watermarks = watermarks;
            StartTime = startTime;
        }

        public IImmutableDictionary<string, long> Watermarks { get; }

        public DateTimeOffset StartTime { get; }

        public string? SourceOperatorId { get; internal set; }

        public override bool Equals(object? obj)
        {
            return obj is Watermark watermark &&
                   EqualityComparer<IImmutableDictionary<string, long>>.Default.Equals(Watermarks, watermark.Watermarks);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Watermarks);
        }
    }
}
