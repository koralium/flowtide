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

using System.Collections;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    internal class EmptyDeleteVector : IDeleteVector
    {
        public static readonly EmptyDeleteVector Instance = new EmptyDeleteVector();

        public long Cardinality => 0;

        public bool Contains(long index)
        {
            return false;
        }

        private IEnumerable<long> Empty()
        {
            yield break;
        }

        public IEnumerator<long> GetEnumerator()
        {
            return Empty().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return Empty().GetEnumerator();
        }
    }
}
