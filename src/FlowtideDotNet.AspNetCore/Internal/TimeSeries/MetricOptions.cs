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

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    public class MetricOptions
    {
        public List<string>? Prefixes { get; set; }

        /// <summary>
        /// Time between capturing the values to time series.
        /// </summary>
        public TimeSpan CaptureRate { get; set; }

        /// <summary>
        /// Set the max lifetime metrics should be kept, if null metrics will be kept forever.
        /// Default is 1 day
        /// </summary>
        public TimeSpan? MaxLifetime { get; set; } = TimeSpan.FromDays(1);
    }
}
