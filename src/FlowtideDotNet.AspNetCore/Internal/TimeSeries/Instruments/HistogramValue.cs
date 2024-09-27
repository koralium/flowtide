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

namespace FlowtideDotNet.AspNetCore.TimeSeries.Instruments
{
    internal class HistogramValue
    {
        public IReadOnlyDictionary<string, string> Tags { get; }

        public double Value { get; set; }

        public long Count { get; set; }

        public HistogramValue(IReadOnlyDictionary<string, string> tags)
        {
            Tags = tags;
        }
    }
}
