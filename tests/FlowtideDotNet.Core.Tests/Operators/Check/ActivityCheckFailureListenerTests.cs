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

using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.Engine;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Tests.Operators.Check
{
    public class ActivityCheckFailureListenerTests
    {
        [Fact]
        public void UserTagsDoNotOverwriteCheckMetadata()
        {
            var checkId = $"{Guid.NewGuid():N}:0";
            var stopped = new List<Activity>();
            using var activityListener = new ActivityListener()
            {
                ShouldListenTo = source => source.Name == "FlowtideDotNet.CheckFailures",
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    // Other tests may report on the same source
                    if ((activity.GetTagItem("flowtide.check.id") as string) == checkId)
                    {
                        lock (stopped)
                        {
                            stopped.Add(activity);
                        }
                    }
                }
            };
            ActivitySource.AddActivityListener(activityListener);

            string streamName = "stream";
            var tags = new KeyValuePair<string, object?>[] { new("CheckId", "user id"), new("Message", "user message"), new("flowtide.check.name", "spoofed"), new("userkey", 17L) };
            var notification = new CheckFailureNotification(ref streamName, checkId, "User {userkey} has no company", tags);
            var listener = new ActivityCheckFailureListener();
            listener.OnCheckFailure(in notification);
            listener.OnCheckResolved(in notification);

            Assert.Equal(new[] { "CheckFailure", "CheckResolved" }, stopped.Select(x => x.OperationName));
            Assert.All(stopped, activity =>
            {
                Assert.Equal("User {userkey} has no company", activity.GetTagItem("flowtide.check.name"));
                Assert.Equal("User 17 has no company", activity.GetTagItem("flowtide.check.message"));
                Assert.Equal("user id", activity.GetTagItem("CheckId"));
                Assert.Equal("user message", activity.GetTagItem("Message"));
                Assert.Equal(17L, activity.GetTagItem("userkey"));
            });
        }
    }
}
