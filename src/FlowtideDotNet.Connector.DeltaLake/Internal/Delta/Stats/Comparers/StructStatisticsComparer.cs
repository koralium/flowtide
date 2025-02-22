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

        public StructStatisticsComparer(IEnumerable<KeyValuePair<string, IStatisticsComparer>> propertyComparers, int? nullCount)
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

            int mapIndex = 0;
            int propertyIndex = 0;

            // Both lists are in sorted order, try and find matching keys
            while (mapIndex < mapLength && propertyIndex < propertyComparers.Count)
            {
                var mapKeyString = mapVal.GetKeyAt(mapIndex).AsString.ToString();
                var comparerKey = propertyComparers[propertyIndex].Key;

                var compareResult = string.Compare(mapKeyString, comparerKey, StringComparison.OrdinalIgnoreCase);

                if (compareResult == 0)
                {
                    var comparer = propertyComparers[propertyIndex].Value;
                    var mapValue = mapVal.GetValueAt(mapIndex);

                    if (!comparer.IsInBetween(mapValue))
                    {
                        return false;
                    }

                    mapIndex++;
                    propertyIndex++;
                }
                else if (compareResult < 0)
                {
                    // The map key is less than the comparer key, so we skip this map key
                    mapIndex++;
                }
                else
                {
                    // The map key is greater than the comparer key, so we skip this comparer key
                    propertyIndex++;
                }
            }

            return false;
        }

        public void WriteMaxValue(Utf8JsonWriter writer, string propertyName)
        {
            writer.WriteStartObject(propertyName);

            for (int i = 0; i < propertyComparers.Count; i++)
            {
                propertyComparers[i].Value.WriteMaxValue(writer, propertyComparers[i].Key);
            }

            writer.WriteEndObject();
        }

        public void WriteMinValue(Utf8JsonWriter writer, string propertyName)
        {
            writer.WriteStartObject(propertyName);

            for (int i = 0; i < propertyComparers.Count; i++)
            {
                propertyComparers[i].Value.WriteMinValue(writer, propertyComparers[i].Key);
            }

            writer.WriteEndObject();
        }

        public void WriteNullValue(Utf8JsonWriter writer, string propertyName)
        {
            writer.WriteStartObject(propertyName);

            for (int i = 0; i < propertyComparers.Count; i++)
            {
                propertyComparers[i].Value.WriteNullValue(writer, propertyComparers[i].Key);
            }

            writer.WriteEndObject();
        }
    }
}
