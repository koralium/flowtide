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

using FlowtideDotNet.Connector.DeltaLake.Internal;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class TruncationCheckScheduleTests
    {
        [Fact]
        public void AnIdleSourceListsEverLessOften()
        {
            var schedule = new TruncationCheckSchedule(TimeSpan.FromSeconds(10));

            Assert.Equal(new[] { 1, 7, 19, 43, 91 }, Checks(schedule, 100));
        }

        [Fact]
        public void ALoadedVersionStartsOverWithoutAListing()
        {
            var schedule = new TruncationCheckSchedule(TimeSpan.FromSeconds(10));
            Checks(schedule, 100);

            schedule.Loaded();

            Assert.Equal(new[] { 6, 12, 24, 48, 96 }, Checks(schedule, 100));
        }

        [Fact]
        public void TheGapStopsGrowingAtAboutAnHourOfPolls()
        {
            // 360 polls an hour
            var schedule = new TruncationCheckSchedule(TimeSpan.FromSeconds(10));
            var checks = Checks(schedule, 5000);

            var gaps = checks.Zip(checks.Skip(1), (a, b) => b - a).ToList();
            Assert.Equal(360, gaps.Max());
            Assert.Equal(360, gaps[^1]);
            Assert.Equal(gaps.OrderBy(x => x), gaps);
        }

        [Fact]
        public void APollSlowerThanTheFirstGapKeepsTheFirstGap()
        {
            var schedule = new TruncationCheckSchedule(TimeSpan.FromMinutes(20));

            Assert.Equal(new[] { 1, 7, 13, 19, 25 }, Checks(schedule, 30));
        }

        private static List<int> Checks(TruncationCheckSchedule schedule, int misses)
        {
            var checks = new List<int>();
            for (int miss = 1; miss <= misses; miss++)
            {
                if (schedule.Miss())
                {
                    checks.Add(miss);
                }
            }
            return checks;
        }
    }
}
