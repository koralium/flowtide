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
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using Parquet;
using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Encoders
{
    internal class StructEncoder : IParquetEncoder
    {
        private readonly List<IParquetEncoder> inner;
        private readonly List<IDataValue> propertyNames;

        public StructEncoder(List<IParquetEncoder> inner, List<IDataValue> propertyNames)
        {
            this.inner = inner;
            this.propertyNames = propertyNames;
        }

        public void FinishBatch()
        {
        }

        public void NewBatch(ParquetSchema schema, Dictionary<string, string>? partitionValues)
        {
            foreach(var i in inner)
            {
                i.NewBatch(schema, partitionValues);
            }
        }

        public async Task NewRowGroup(IParquetRowGroupReader rowGroupReader)
        {
            foreach (var i in inner)
            {
                await i.NewRowGroup(rowGroupReader);
            }
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
