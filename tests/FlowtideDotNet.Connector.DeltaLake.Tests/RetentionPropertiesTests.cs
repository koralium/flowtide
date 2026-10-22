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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class RetentionPropertiesTests
    {
        private static Dictionary<string, string> Both(string value) => new Dictionary<string, string>()
        {
            [RetentionProperties.LogKey] = value,
            [RetentionProperties.DeletedFileKey] = value
        };

        [Theory]
        [InlineData(24, "interval 24 hours")]
        [InlineData(36, "interval 36 hours")]
        [InlineData(168, "interval 168 hours")]
        public void FormatWritesWholeHours(long hours, string expected)
        {
            Assert.Equal(expected, RetentionProperties.Format(TimeSpan.FromHours(hours)));
        }

        // Up to the largest whole-hours retention validation admits
        [Fact]
        public void FormatRoundTripsThroughTheIntervalParserUpToTheLargestRetention()
        {
            var largest = TimeSpan.FromTicks(TimeSpan.MaxValue.Ticks / TimeSpan.TicksPerHour * TimeSpan.TicksPerHour);
            foreach (var retention in new[] { TimeSpan.FromDays(1), TimeSpan.FromDays(30), largest })
            {
                Assert.Equal(retention, DeltaInterval.Parse(RetentionProperties.Format(retention)));
                Assert.Equal(retention, CheckpointExpiry.Retention(Both(RetentionProperties.Format(retention))));
            }
        }

        [Fact]
        public void MatchesNeedsBothKeysWithTheExactValue()
        {
            var formatted = RetentionProperties.Format(TimeSpan.FromDays(7));
            Assert.True(RetentionProperties.Matches(Both(formatted), formatted));
            Assert.False(RetentionProperties.Matches(null, formatted));
            Assert.False(RetentionProperties.Matches(new Dictionary<string, string>(), formatted));
            Assert.False(RetentionProperties.Matches(new Dictionary<string, string>() { [RetentionProperties.LogKey] = formatted }, formatted));
            Assert.False(RetentionProperties.Matches(new Dictionary<string, string>() { [RetentionProperties.DeletedFileKey] = formatted }, formatted));
            Assert.False(RetentionProperties.Matches(new Dictionary<string, string>() { [RetentionProperties.LogKey] = formatted, [RetentionProperties.DeletedFileKey] = "interval 1 hours" }, formatted));
        }

        // Another spelling of the same duration
        [Theory]
        [InlineData("interval 7 days")]
        [InlineData("INTERVAL 168 HOURS")]
        [InlineData(" interval 168 hours")]
        [InlineData("interval 168 hour")]
        public void MatchesComparesTheStringExactly(string value)
        {
            Assert.False(RetentionProperties.Matches(Both(value), RetentionProperties.Format(TimeSpan.FromDays(7))));
        }

        [Fact]
        public void MatchesComparesKeysExactly()
        {
            var formatted = RetentionProperties.Format(TimeSpan.FromDays(7));
            var configuration = new Dictionary<string, string>()
            {
                ["Delta.LogRetentionDuration"] = formatted,
                [RetentionProperties.DeletedFileKey] = formatted
            };
            Assert.False(RetentionProperties.Matches(configuration, formatted));
        }
    }
}
