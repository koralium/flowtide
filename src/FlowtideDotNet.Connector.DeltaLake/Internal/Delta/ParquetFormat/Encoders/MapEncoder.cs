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
using Parquet;
using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Encoders
{
    internal class MapEncoder : IParquetEncoder
    {
        private readonly IParquetEncoder keyEncoder;
        private readonly IParquetEncoder valueEncoder;
        private readonly int _repititionLevel;
        private readonly int _definitionLevel;

        private int repititionIndex = 0;
        private int dataIndex = 0;

        public MapEncoder(IParquetEncoder keyEncoder, IParquetEncoder valueEncoder, int repititionLevel, int definitionLevel)
        {
            this.keyEncoder = keyEncoder;
            this.valueEncoder = valueEncoder;
            this._repititionLevel = repititionLevel;
            this._definitionLevel = definitionLevel;
        }

        public void FinishBatch()
        {
            keyEncoder.FinishBatch();
            valueEncoder.FinishBatch();
        }

        public void NewBatch(ParquetSchema schema, Dictionary<string, string>? partitionValues)
        {
            keyEncoder.NewBatch(schema, partitionValues);
            valueEncoder.NewBatch(schema, partitionValues);
        }

        public async Task NewRowGroup(IParquetRowGroupReader rowGroupReader)
        {
            await keyEncoder.NewRowGroup(rowGroupReader);
            await valueEncoder.NewRowGroup(rowGroupReader);
        }

        public void ReadNextData(ref AddToColumnParquet addToColumnFunc)
        {
            throw new NotImplementedException();
        }

        public (int repititionLevel, int nextRepititionLevel, int definitionLevel) ReadNextRepitionAndDefinition()
        {
            throw new NotImplementedException();
        }

        public void SkipNextIndex()
        {
            throw new NotImplementedException();
        }
    }
}
