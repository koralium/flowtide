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
using FlowtideDotNet.Core.ColumnStore.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class EventBatchWeighted : IRentable
    {
        private readonly PrimitiveList<int> weights;
        private readonly PrimitiveList<uint> iterations;
        private readonly EventBatchData eventBatchData;

        public EventBatchData EventBatchData => eventBatchData;
        public PrimitiveList<int> Weights => weights;
        public PrimitiveList<uint> Iterations => iterations;

        public int Count => weights.Count;

        public EventBatchWeighted(PrimitiveList<int> weights, PrimitiveList<uint> iterations, EventBatchData eventBatchData)
        {
            this.weights = weights;
            this.iterations = iterations;
            this.eventBatchData = eventBatchData;
        }

        public void Rent(int count)
        {
            weights.Rent(count);
            iterations.Rent(count);
            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                eventBatchData.Columns[i].Rent(count);
            }
        }

        public void Return()
        {
            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                eventBatchData.Columns[i].Return();
            }
            iterations.Return();
            weights.Return();
        }
    }
}
