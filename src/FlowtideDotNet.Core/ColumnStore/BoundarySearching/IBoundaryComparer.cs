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

namespace FlowtideDotNet.Core.ColumnStore.BoundarySearching
{
    internal interface IBoundaryComparer
    {
        /// <summary>
        /// Returns < 0 if tree is less, 0 if equal, > 0 if tree is greater
        /// </summary>
        /// <param name="treeIndex">Index of the row in the tree node</param>
        /// <param name="inputIndex">Index of the row of the incoming batch</param>
        /// <returns></returns>
        int CompareTreeToInput(int treeIndex, int inputIndex);

        int CompareInputToInput(int inputIndexA, int inputIndexB);
    }
}
