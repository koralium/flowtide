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

using Apache.Arrow;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ArrowEncoders;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ArrowPartitionEncoders
{
    internal class StringPartitionEncoder : IArrowEncoder
    {
        private readonly string physicalName;
        private StringValue? _partitionValue;

        public StringPartitionEncoder(string physicalName)
        {
            this.physicalName = physicalName;
        }

        public bool IsPartitionValueEncoder => true;

        public void AddValue(int index, ref AddToColumnFunc func)
        {
            if (_partitionValue == null)
            {
                func.AddValue(NullValue.Instance);
            }
            else
            {
                func.AddValue(_partitionValue.Value);
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

            _partitionValue = new StringValue(value);
        }
    }
}
