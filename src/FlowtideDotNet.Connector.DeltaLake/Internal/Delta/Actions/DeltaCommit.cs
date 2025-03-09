using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
