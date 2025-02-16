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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Encoders;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using Parquet;
using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.PartitionValueEncoders
{
    internal class DecimalPartitionEncoder : IParquetEncoder
    {
        private readonly string physicalName;
        private DecimalValue? _value;

        public DecimalPartitionEncoder(string physicalName)
        {
            this.physicalName = physicalName;
        }

        public void FinishBatch()
        {
            _value = null;
        }

        public void NewBatch(ParquetSchema schema, Dictionary<string, string>? partitionValues)
        {
            if (partitionValues == null)
            {
                throw new ArgumentNullException(nameof(partitionValues));
            }

            if (!partitionValues.TryGetValue(physicalName, out var value))
            {
                throw new ArgumentException($"Partition value for {physicalName} not found");
            }

            if (!decimal.TryParse(value, CultureInfo.InvariantCulture, out var decimalValue))
            {
                throw new ArgumentException($"Partition value for {physicalName} is not a decimal");
            }

            _value = new DecimalValue(decimalValue);
        }

        public Task NewRowGroup(IParquetRowGroupReader rowGroupReader)
        {
            return Task.CompletedTask;
        }

        public void ReadNextData(ref AddToColumnParquet addToColumnFunc)
        {
            if (_value == null)
            {
                addToColumnFunc.AddValue(NullValue.Instance);
            }
            else
            {
                addToColumnFunc.AddValue(_value.Value);
            }
        }

        public (int repititionLevel, int nextRepititionLevel, int definitionLevel) ReadNextRepitionAndDefinition()
        {
            return (0, 0, 0);
        }

        public void SkipNextIndex()
        {
            throw new NotImplementedException();
        }
    }
}
