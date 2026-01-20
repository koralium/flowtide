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
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Storage.Memory;
using Stowage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.CheckpointReading
{
    internal class ParquetCheckpointReader
    {
        public async Task<List<DeltaAction>> ReadCheckpointFile(IFileStorage storage, IOEntry path)
        {
            var stream = await storage.OpenRead(path.Path);

            if (stream == null)
            {
                throw new Exception($"File not found: {path}");
            }

            ParquetSharp.Arrow.FileReader fileReader = new ParquetSharp.Arrow.FileReader(stream);

            var batchReader = fileReader.GetRecordBatchReader();

            List<DeltaAction> actions = new List<DeltaAction>();
            Apache.Arrow.RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
            {
                for (int i = 0; i < batch.Length; i++)
                {
                    for (int c = 0; c < batch.ColumnCount; c++)
                    {
                        var field = batch.Schema.FieldsList[c];
                        var col = batch.Column(c);
                        
                        if (!col.IsNull(i))
                        {
                            CheckpointReadVisitor checkpointReadVisitor = new CheckpointReadVisitor(field, i);
                            var action = checkpointReadVisitor.GetAction(col);
                            if (action != null)
                            {
                                actions.Add(action);
                            }
                        }
                    }
                }
            }

            return actions;
        }
    }
}
