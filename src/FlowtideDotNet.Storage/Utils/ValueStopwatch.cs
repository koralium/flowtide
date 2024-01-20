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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Utils
{
    public readonly struct ValueStopwatch
    {
        private static readonly double s_timestampToTicks = TimeSpan.TicksPerSecond / (double)Stopwatch.Frequency;

        private readonly long _startTimestamp;

        private ValueStopwatch(long startTimestamp) => _startTimestamp = startTimestamp;

        public static ValueStopwatch StartNew() => new ValueStopwatch(GetTimestamp());

        public static long GetTimestamp() => Stopwatch.GetTimestamp();

        public static TimeSpan GetElapsedTime(long startTimestamp, long endTimestamp)
        {
            var timestampDelta = endTimestamp - startTimestamp;
            var ticks = (long)(s_timestampToTicks * timestampDelta);
            return new TimeSpan(ticks);
        }

        public TimeSpan GetElapsedTime() => GetElapsedTime(_startTimestamp, GetTimestamp());
    }

}
