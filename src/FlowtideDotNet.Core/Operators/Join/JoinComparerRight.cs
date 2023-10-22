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

namespace FlowtideDotNet.Core.Operators.Join
{
    internal class JoinComparerRight : IComparer<JoinStreamEvent>
    {
        private readonly Func<JoinStreamEvent, JoinStreamEvent, int> indexComparer;
        private readonly Func<JoinStreamEvent, JoinStreamEvent, int> seekComparer;

        public JoinComparerRight(Func<JoinStreamEvent, JoinStreamEvent, int> indexComparer, Func<JoinStreamEvent, JoinStreamEvent, int> seekComparer)
        {
            this.indexComparer = indexComparer;
            this.seekComparer = seekComparer;
        }

        public int Compare(JoinStreamEvent x, JoinStreamEvent y)
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
                if (seekComparer(x, y) <= 0)
                {
                    return -1;
                }
                return 1;
            }
            if (y.TargetId == 1)
            {
                if (seekComparer(y, x) <= 0)
                {
                    return 1;
                }
                return -1;
            }
            throw new NotImplementedException();
        }
    }
}
