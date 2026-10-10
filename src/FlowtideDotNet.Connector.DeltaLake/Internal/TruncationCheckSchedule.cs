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

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    /// <summary>
    /// Which polls that miss the next commit list the log to look for a removed commit.
    /// The first miss after a start lists, then the gap between listings doubles up to about an hour of polls,
    /// a loaded version starts over so catching up never lists.
    /// </summary>
    internal sealed class TruncationCheckSchedule
    {
        public const int FirstGap = 6;

        private readonly int _maxGap;
        private long _misses;
        private long _nextCheck = 1;
        private int _gap = FirstGap;

        public TruncationCheckSchedule(TimeSpan pollInterval)
        {
            var pollsPerHour = pollInterval > TimeSpan.Zero ? TimeSpan.FromHours(1) / pollInterval : FirstGap;
            _maxGap = (int)Math.Clamp(pollsPerHour, FirstGap, int.MaxValue / 4);
        }

        // True when this miss lists the log
        public bool Miss()
        {
            _misses++;
            if (_misses < _nextCheck)
            {
                return false;
            }
            _nextCheck = _misses + _gap;
            _gap = Math.Min(_gap * 2, _maxGap);
            return true;
        }

        public void Loaded()
        {
            _misses = 0;
            _nextCheck = FirstGap;
            _gap = FirstGap;
        }
    }
}
