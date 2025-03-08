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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    /// <summary>
    /// Iterates through two deletion vectors and returns differences.
    /// </summary>
    internal class DeletionVectorDiffIterator : IEnumerable<(long id, int weight)>
    {
        private readonly IDeleteVector removed;
        private readonly IDeleteVector added;

        public DeletionVectorDiffIterator(IDeleteVector removed, IDeleteVector added)
        {
            this.removed = removed;
            this.added = added;
        }

        private IEnumerable<(long id, int weight)> GetDifference()
        {
            var removedEnumerator = removed.GetEnumerator();
            var addedEnumerator = added.GetEnumerator();

            bool removedHasNext = removedEnumerator.MoveNext();
            bool addedHasNext = addedEnumerator.MoveNext();

            while (removedHasNext && addedHasNext)
            {
                if (removedEnumerator.Current < addedEnumerator.Current)
                {
                    yield return (removedEnumerator.Current,  1);
                    removedHasNext = removedEnumerator.MoveNext();
                }
                else if (removedEnumerator.Current == addedEnumerator.Current)
                {
                    // Same, so no difference, no need to yield any data
                    removedHasNext = removedEnumerator.MoveNext();
                    addedHasNext = addedEnumerator.MoveNext();
                }
                else if (removedEnumerator.Current > addedEnumerator.Current)
                {
                    yield return (addedEnumerator.Current, -1);
                    addedHasNext = addedEnumerator.MoveNext();
                }
            }

            while (removedHasNext)
            {
                yield return (removedEnumerator.Current, 1);
                removedHasNext = removedEnumerator.MoveNext();
            }

            while (addedHasNext)
            {
                yield return (addedEnumerator.Current, -1);
                addedHasNext = addedEnumerator.MoveNext();
            }
        }

        public IEnumerator<(long id, int weight)> GetEnumerator()
        {
            return GetDifference().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetDifference().GetEnumerator();
        }
    }
}
