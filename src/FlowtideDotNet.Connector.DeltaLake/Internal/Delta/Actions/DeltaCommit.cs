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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions
{
    internal class DeltaCommit
    {
        public List<DeltaAddAction> AddedFiles { get; }

        public List<DeltaRemoveFileAction> RemovedFiles { get; }
        public List<DeltaCdcAction> CdcFiles { get; }
        public DeltaMetadataAction? UpdatedMetadata { get; }

        public DeltaCommit(
            List<DeltaAddAction> addedFiles,
            List<DeltaRemoveFileAction> removedFiles,
            List<DeltaCdcAction> cdcFiles,
            DeltaMetadataAction? updatedMetadata)
        {
            AddedFiles = addedFiles;
            RemovedFiles = removedFiles;
            CdcFiles = cdcFiles;
            UpdatedMetadata = updatedMetadata;
        }
    }
}
