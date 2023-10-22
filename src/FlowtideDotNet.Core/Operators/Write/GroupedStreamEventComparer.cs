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

namespace FlowtideDotNet.Core.Operators.Write
{
    internal class GroupedStreamEventComparer : IComparer<GroupedStreamEvent>
    {
        private readonly Func<GroupedStreamEvent, GroupedStreamEvent, int> indexComparer;

        public GroupedStreamEventComparer(Func<GroupedStreamEvent, GroupedStreamEvent, int> indexComparer)
        {
            this.indexComparer = indexComparer;
        }

        public int Compare(GroupedStreamEvent x, GroupedStreamEvent y)
        {
            if (x.TargetId == 0 && y.TargetId == 0)
            {
                var comp = indexComparer(x, y);
                if (comp != 0)
                {
                    return comp;
                }
                return x.Span.SequenceCompareTo(y.Span);
            }
            if (x.TargetId == 1)
            {
                var i = indexComparer(y, x);
                //If the value in the index is greater or equal to the value in the 
                if (i >= 0)
                {
                    return -1;
                }
                return 1;
            }
            else if (y.TargetId == 1)
            {
                var i = indexComparer(x, y);
                // If the value in the index is greater or equal to the lookup value, return that it is greater
                if (i >= 0)
                {
                    return 1;
                }
                return -1;
            }
            return indexComparer(x, y);
        }
    }
}
