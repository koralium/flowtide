﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Engine
{
    internal class ActivityCheckFailureListener : ICheckFailureListener
    {
        private static readonly ActivitySource _activitySource = new ActivitySource("FlowtideDotNet.CheckFailures");
        public void OnCheckFailure(ref readonly CheckFailureNotification notification)
        {
            var activity = _activitySource.StartActivity("CheckFailure", ActivityKind.Internal);
            if (activity != null)
            {
                activity.DisplayName = $"Check failed with message: {notification.Message}";
                activity.SetTag("Message", notification.Message);
                foreach (var tag in notification.Tags)
                {
                    activity.SetTag(tag.Key, tag.Value);
                }
                activity.Stop();
            }
        }
    }
}
