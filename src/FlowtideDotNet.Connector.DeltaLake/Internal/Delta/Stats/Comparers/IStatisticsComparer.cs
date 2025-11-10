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
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers
{
    /// <summary>
    /// Represents a comparer that checks min and max values of against a value.
    /// They are used to help filter out data files to search valeus in.
    /// </summary>
    internal interface IStatisticsComparer
    {
        bool IsInBetween<T>(T value)
            where T : IDataValue;


        void WriteMinValue(Utf8JsonWriter writer, string propertyName);

        void WriteMaxValue(Utf8JsonWriter writer, string propertyName);

        void WriteNullValue(Utf8JsonWriter writer, string propertyName);

        int? NullCount { get; }

        byte[]? GetMinValueIcebergBinary();

        byte[]? GetMaxValueIcebergBinary();
    }
}
