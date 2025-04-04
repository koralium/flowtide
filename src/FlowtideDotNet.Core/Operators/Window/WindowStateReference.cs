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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Window
{
    internal class WindowStateReference
    {
        private ColumnRowReference? _columnRowReference;
        public int weightIndex;
        private WindowValue windowValue;
        private IWindowAddOutputRow? _addOutputRow;

        public WindowStateReference(IWindowAddOutputRow? addOutputRow)
        {
            _addOutputRow = addOutputRow;
        }

        public void ResetRow(ColumnRowReference columnRowReference, int weightIndex, WindowValue windowValue)
        {
            _columnRowReference = columnRowReference;
            this.weightIndex = weightIndex;
            this.windowValue = windowValue;
        }

        public void ResetPage()
        {
            Updated = false;
        }

        internal bool Updated { get; set; }

        public void UpdateStateValue<T>(T value)
            where T : IDataValue
        {
            Debug.Assert(_columnRowReference != null);
            if (_addOutputRow == null)
            {
                throw new InvalidOperationException("Can not update state using this iterator");
            }

            if (windowValue.UpdateStateValue(0, weightIndex, value, _columnRowReference.Value, _addOutputRow))
            {
                Updated = true;
            }
        }
    }
}
