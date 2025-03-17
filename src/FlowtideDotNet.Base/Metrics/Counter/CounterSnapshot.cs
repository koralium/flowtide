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

namespace FlowtideDotNet.Base.Metrics.Counter
{
    public class CounterSnapshot
    {
        public CounterSnapshot(
            string name,
            string? unit,
            string? description,
            CounterTagSnapshot total,
            bool isMultiDimensional,
            IReadOnlyDictionary<string, CounterTagSnapshot> tagValues)
        {
            Name = name;
            Unit = unit;
            Description = description;
            Total = total;
            IsMultiDimensional = isMultiDimensional;
            TagValues = tagValues;
        }

        public string Name { get; }

        public string? Unit { get; }

        public string? Description { get; }

        public CounterTagSnapshot Total { get; }

        public bool IsMultiDimensional { get; }

        public IReadOnlyDictionary<string, CounterTagSnapshot> TagValues { get; }
    }
}
