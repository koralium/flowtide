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

namespace FlowtideDotNet.Base
{
    /// <summary>
    /// Represents an object or resource whose lifetime is managed through reference counting.
    /// Used heavily to track memory allocations across the stream and return resources to pools.
    /// </summary>
    public interface IRentable
    {
        /// <summary>
        /// Increases the reference count of the rentable resource by the specified amount.
        /// Call this when the resource is passed to a new scope or consumer that will rely on it.
        /// </summary>
        /// <param name="count">The number of additional references to acquire.</param>
        void Rent(int count);

        /// <summary>
        /// Decreases the reference count of the rentable resource by one.
        /// When the count reaches zero, the underlying resource may be disposed, freed, or returned to a pool.
        /// </summary>
        void Return();
    }
}
