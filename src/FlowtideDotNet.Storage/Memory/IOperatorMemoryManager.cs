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

namespace FlowtideDotNet.Storage.Memory
{
    /// <summary>
    /// Used in an operator to manage memory allocation and deallocation.
    /// Exist in operator level to allow gathering of metrics how much memory each operator uses.
    /// </summary>
    public interface IOperatorMemoryManager : IMemoryAllocator
    {
    }
}
