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
using System.Diagnostics;

namespace FlowtideDotNet.Core.Engine
{
    internal class ActivityCheckFailureListener : ICheckFailureListener
    {
        private static readonly ActivitySource _activitySource = new ActivitySource("FlowtideDotNet.CheckFailures");

        /// <inheritdoc/>
        public void OnCheckFailure(ref readonly CheckFailureNotification notification)
        {
            ReportActivity("CheckFailure", "Check failed with message: ", in notification);
        }

        /// <inheritdoc/>
        public void OnCheckResolved(ref readonly CheckFailureNotification notification)
        {
            ReportActivity("CheckResolved", "Check resolved with message: ", in notification);
        }

        /// <inheritdoc/>
        public void OnCheckReset(ref readonly CheckResetNotification notification)
        {
        }

        private static void ReportActivity(string name, string displayPrefix, ref readonly CheckFailureNotification notification)
        {
            var activity = _activitySource.StartActivity(name, ActivityKind.Internal);
            if (activity != null)
            {
                var message = CheckNameRenderer.Render(notification.CheckName, notification.Tags);
                activity.DisplayName = displayPrefix + message;
                activity.SetTag("CheckId", notification.CheckId);
                activity.SetTag("CheckName", notification.CheckName);
                activity.SetTag("Message", message);
                foreach (var tag in notification.Tags)
                {
                    activity.SetTag(tag.Key, tag.Value);
                }
                activity.Stop();
            }
        }
    }
}
