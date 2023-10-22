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

using App.Metrics.Concurrency;

namespace FlowtideDotNet.Base.Metrics.Counter
{
    internal class CounterMetricFloating : ICounterMetric
    {
        private sealed class Bucket
        {
            public AtomicDouble _count;
            public long _createdTime;

            public Bucket()
            {
                _count = new AtomicDouble();
                _createdTime = long.MinValue;
            }

            public void Reset(long createdTime, double previousValue)
            {
                _count.Decrement(previousValue);
                _createdTime = createdTime;
            }

            public void Add(double value)
            {
                _count.Add(value);
            }
        }

        private readonly Bucket[] _buckets;
        private readonly object _lock;
        private readonly Func<long> timeSecondsFunc;
        private AtomicDouble _sum;

        public CounterMetricFloating(Func<long>? timeSecondsFunc = null)
        {
            _lock = new object();
            _buckets = new Bucket[60];
            for (int i = 0; i < _buckets.Length; i++)
            {
                _buckets[i] = new Bucket();
            }

            if (timeSecondsFunc != null)
            {
                this.timeSecondsFunc = timeSecondsFunc;
            }
            else
            {
                this.timeSecondsFunc = DateTimeOffset.UtcNow.ToUnixTimeSeconds;
            }
        }

        public void Add<T>(T value)
        {
            var convertedVal = Convert.ToDouble(value);
            var unixSeconds = timeSecondsFunc();
            var modVal = (int)(unixSeconds % 60);

            var bucket = _buckets[modVal];

            var compareSeconds = unixSeconds - 1;
            if (bucket._createdTime < compareSeconds)
            {
                lock (_lock)
                {
                    if (bucket._createdTime < compareSeconds)
                    {
                        var previousValue = bucket._count.GetValue();
                        bucket.Reset(unixSeconds, previousValue);
                    }
                }
            }
            _sum.Add(convertedVal);
            bucket.Add(convertedVal);
        }

        public decimal GetRatePerMinute()
        {
            var currentUnixSeconds = timeSecondsFunc();
            var minAllowedValue = currentUnixSeconds - 60;

            decimal sum = 0;

            foreach (var bucket in _buckets)
            {
                if (bucket._createdTime >= minAllowedValue)
                {
                    sum += (decimal)bucket._count.GetValue();
                }
            }

            return sum;
        }

        public CounterTagSnapshot GetSnapshot()
        {
            var rate = GetRatePerMinute();
            var sum = (decimal)_sum.GetValue();

            return new CounterTagSnapshot(sum, rate);
        }
    }
}
