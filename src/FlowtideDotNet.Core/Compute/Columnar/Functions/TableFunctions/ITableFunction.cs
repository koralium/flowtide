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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.StateManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.TableFunctions
{
    public interface ITableFunction
    {

        /// <summary>
        /// Recieve an event batch of data and return an async enumerable of event batches.
        /// This allows batching multiple input rows in the same table function call.
        /// </summary>
        /// <param name="eventBatch"></param>
        /// <returns></returns>
        IAsyncEnumerable<EventBatchWeighted> OnRecieve(EventBatchWeighted eventBatch);

        IAsyncEnumerable<EventBatchWeighted> OnTrigger(string name, object? state);

        IAsyncEnumerable<EventBatchWeighted> OnWatermark(Watermark watermark);

        Task OnCheckpoint();

        Task Initialize(IStateManagerClient stateManagerClient, Func<string, TimeSpan?, Task> registerTrigger);
    }
}
