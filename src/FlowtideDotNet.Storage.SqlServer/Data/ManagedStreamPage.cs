﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.SqlServer.Data
{
    [StructLayout(LayoutKind.Sequential)]
    internal record struct ManagedStreamPage(long PageId, int Version, Guid PageKey, bool ShouldDelete = false)
    {
        /// <summary>
        /// Creates a copy of the current <see cref="ManagedStreamPage"/> with <see cref="ShouldDelete"/> set to <c>true</c>.
        /// </summary>
        public readonly ManagedStreamPage CopyAsDeleted()
        {
            return new(PageId, Version, PageKey, true);
        }

        /// <summary>
        /// Creates a copy of the current <see cref="ManagedStreamPage"/> with <see cref="ShouldDelete"/> set to <c>false</c>.
        /// </summary>
        public readonly ManagedStreamPage CopyAsNotDeleted()
        {
            return new(PageId, Version, PageKey, false);
        }
    }
}
