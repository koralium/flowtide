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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.ObjectStorage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.BlobStore
{
    public class BlobNewCheckpointTests
    {
        [Fact]
        public void AddPagesToEmptyCheckpointInOrder()
        {
            var checkpoint = new BlobNewCheckpoint(GlobalMemoryManager.Instance);

            checkpoint.AddUpsertPages(
                new Storage.DataStructures.PrimitiveList<long>(GlobalMemoryManager.Instance)
                {
                    1, 2, 3, 4, 5
                },
                new Storage.DataStructures.PrimitiveList<long>(GlobalMemoryManager.Instance)
                {
                    10, 20, 30, 40, 50
                },
                new Storage.DataStructures.PrimitiveList<int>(GlobalMemoryManager.Instance)
                {
                    100, 200, 300, 400, 500
                }
            );
        }

        [Fact]
        public void AddPagesToEmptyCheckpointUnordered()
        {
            var checkpoint = new BlobNewCheckpoint(GlobalMemoryManager.Instance);

            checkpoint.AddUpsertPages(
                new Storage.DataStructures.PrimitiveList<long>(GlobalMemoryManager.Instance)
                {
                    5, 2, 1, 4, 3
                },
                new Storage.DataStructures.PrimitiveList<long>(GlobalMemoryManager.Instance)
                {
                    10, 20, 30, 40, 50
                },
                new Storage.DataStructures.PrimitiveList<int>(GlobalMemoryManager.Instance)
                {
                    100, 200, 300, 400, 500
                }
            );
        }

        [Fact]
        public void AddPagesToNonEmptyCheckpointUnordered()
        {
            var checkpoint = new BlobNewCheckpoint(GlobalMemoryManager.Instance);

            checkpoint.AddUpsertPages(
                new Storage.DataStructures.PrimitiveList<long>(GlobalMemoryManager.Instance)
                {
                    1, 3, 5, 7, 9
                },
                new Storage.DataStructures.PrimitiveList<long>(GlobalMemoryManager.Instance)
                {
                    10, 20, 30, 40, 50
                },
                new Storage.DataStructures.PrimitiveList<int>(GlobalMemoryManager.Instance)
                {
                    100, 200, 300, 400, 500
                }
            );

            checkpoint.AddUpsertPages(
                new Storage.DataStructures.PrimitiveList<long>(GlobalMemoryManager.Instance)
                {
                    6, 4, 2, 8
                },
                new Storage.DataStructures.PrimitiveList<long>(GlobalMemoryManager.Instance)
                {
                    60, 70, 80, 90
                },
                new Storage.DataStructures.PrimitiveList<int>(GlobalMemoryManager.Instance)
                {
                    600, 700, 800, 900
                }
            );
        }
    }
}
