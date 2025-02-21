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

using FlowtideDotNet.Core.ColumnStore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers
{
    internal class StructStatisticsComparer : IStatisticsComparer
    {
        private readonly List<KeyValuePair<string, IStatisticsComparer>> propertyComparers;
        private readonly int? nullCount;

        public StructStatisticsComparer(List<KeyValuePair<string, IStatisticsComparer>> propertyComparers, int? nullCount)
        {
            this.propertyComparers = propertyComparers.OrderBy(x => x.Key).ToList();
            this.nullCount = nullCount;
        }

        public bool IsInBetween<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                if ((!nullCount.HasValue) || nullCount > 0)
                {
                    return true;
                }
                return false;
            }

            var mapVal = value.AsMap;
            var mapLength = mapVal.GetLength();
            for (int i = 0; i < propertyComparers.Count; i++)
            {

            }

            throw new NotImplementedException();
        }

        public void WriteMaxValue(Utf8JsonWriter writer, string propertyName)
        {
            throw new NotImplementedException();
        }

        public void WriteMinValue(Utf8JsonWriter writer, string propertyName)
        {
            throw new NotImplementedException();
        }

        public void WriteNullValue(Utf8JsonWriter writer, string propertyName)
        {
            throw new NotImplementedException();
        }
    }
}
