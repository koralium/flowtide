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

using FlowtideDotNet.Storage.DataStructures;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    internal abstract class PagesFile : PipeReader
    {
        public abstract PrimitiveList<long> PageIds { get; }

        public abstract PrimitiveList<int> PageOffsets { get; }

        public abstract PrimitiveList<uint> Crc32s { get; }

        public abstract int FileSize { get; }

        /// <summary>
        /// The crc64 of the file
        /// </summary>
        public abstract ulong Crc64 { get; }

        public abstract void Return();

        public abstract void DoneWriting();
    }
}
