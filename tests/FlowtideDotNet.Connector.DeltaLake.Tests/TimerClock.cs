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

using Microsoft.Extensions.Time.Testing;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    /// <summary>
    /// A fake clock that signals every timer created on it, so a test advances only once a wait has started.
    /// </summary>
    internal sealed class TimerClock : TimeProvider
    {
        private readonly SemaphoreSlim _created = new SemaphoreSlim(0);

        public TimerClock(DateTimeOffset start)
        {
            Fake = new FakeTimeProvider(start);
        }

        public FakeTimeProvider Fake { get; }

        public override DateTimeOffset GetUtcNow() => Fake.GetUtcNow();

        public override long GetTimestamp() => Fake.GetTimestamp();

        public override long TimestampFrequency => Fake.TimestampFrequency;

        public override TimeZoneInfo LocalTimeZone => Fake.LocalTimeZone;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = Fake.CreateTimer(callback, state, dueTime, period);
            _created.Release();
            return timer;
        }

        /// <summary>
        /// Completes once one more timer was created; throws when none is within the wait.
        /// </summary>
        public async Task NextTimerAsync(TimeSpan wait)
        {
            if (!await _created.WaitAsync(wait))
            {
                throw new TimeoutException("No timer was created");
            }
        }
    }
}
