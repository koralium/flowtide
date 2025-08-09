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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    internal interface IColumnAggregateContainer
    {
        ValueTask Compute(ColumnRowReference key, EventBatchData rowBatch, int rowIndex, ColumnReference state, long weight);

        void Disponse();

        Task Commit();

        ValueTask GetValue(ColumnRowReference key, ColumnReference state, Column outputColumn);
    }
}
