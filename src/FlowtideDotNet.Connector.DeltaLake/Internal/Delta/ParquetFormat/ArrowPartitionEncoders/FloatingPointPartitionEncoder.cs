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

using Apache.Arrow;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ArrowEncoders;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using System.Globalization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ArrowPartitionEncoders
{
    internal class FloatingPointPartitionEncoder : IArrowEncoder
    {
        private readonly string physicalName;
        private DoubleValue? _value;

        public FloatingPointPartitionEncoder(string physicalName)
        {
            this.physicalName = physicalName;
        }

        public bool IsPartitionValueEncoder => true;

        public void AddValue(int index, ref AddToColumnFunc func)
        {
            if (_value == null)
            {
                func.AddValue(NullValue.Instance);
            }
            else
            {
                func.AddValue(_value.Value);
            }
        }

        public void NewBatch(IArrowArray arrowArray)
        {
        }

        public void NewFile(Dictionary<string, string>? partitionValues)
        {
            if (partitionValues == null)
            {
                throw new ArgumentNullException(nameof(partitionValues));
            }

            if (!partitionValues.TryGetValue(physicalName, out var value))
            {
                throw new ArgumentException($"Partition value for {physicalName} not found");
            }

            if (!double.TryParse(value, CultureInfo.InvariantCulture, out var doubleValue))
            {
                throw new ArgumentException($"Partition value for {physicalName} is not a double");
            }

            _value = new DoubleValue(doubleValue);
        }

        public void NewNullBatch()
        {
        }
    }
}
