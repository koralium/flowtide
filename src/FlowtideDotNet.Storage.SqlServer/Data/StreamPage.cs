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

namespace FlowtideDotNet.Storage.SqlServer.Data
{
    public record StreamPage(Guid PageKey, int StreamKey, long PageId, byte[] Payload, int Version)
    {
        public bool IsDeleted { get; set; }

        public bool IsPersisted { get; set; }

        public StreamPage CopyAsDeleted()
        {
            return new(PageKey, StreamKey, PageId, [], Version)
            {
                IsDeleted = true,
                IsPersisted = IsPersisted
            };
        }

        public StreamPage CopyAsPersisted()
        {
            return new(PageKey, StreamKey, PageId, Payload, Version)
            {
                IsDeleted = IsDeleted,
                IsPersisted = true
            };
        }
    }
}
