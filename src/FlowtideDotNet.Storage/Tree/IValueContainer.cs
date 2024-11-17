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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree
{
    public interface IValueContainer<V> : IDisposable
    {
        void Insert(int index, V value);

        void Update(int index, V value);

        void RemoveAt(int index);

        int Count { get; }

        V Get(int index);

        ref V GetRef(int index);

        void AddRangeFrom(IValueContainer<V> container, int start, int count);

        void RemoveRange(int start, int count);

        int GetByteSize();

        int GetByteSize(int start, int end);
    }
}
