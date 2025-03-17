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

using FlowtideDotNet.Storage.StateManager.Internal;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal class BPlusTreeSerializerCheckpointContext : IBPlusTreeSerializerCheckpointContext
    {
        private List<long>? m_existingPages;
        private IStateSerializerCheckpointWriter? m_checkpointWriter;
        private bool m_listUpdated;

        public bool ListUpdated => m_listUpdated;

        public void Initialize(List<long> existingPages, IStateSerializerCheckpointWriter checkpointWriter)
        {
            m_existingPages = existingPages;
            m_checkpointWriter = checkpointWriter;
            m_listUpdated = false;
        }

        public Task RemovePage(long pageId)
        {
            Debug.Assert(m_checkpointWriter != null);
            Debug.Assert(m_existingPages != null);
            if (m_existingPages.Contains(pageId))
            {
                m_existingPages.Remove(pageId);
                m_listUpdated = true;
            }
            return m_checkpointWriter.RemovePage(pageId);
        }

        public Memory<byte> RequestPageMemory(int expectedSize)
        {
            Debug.Assert(m_checkpointWriter != null);
            return m_checkpointWriter.RequestPageMemory(expectedSize);
        }

        public Task WritePageMemory(long pageId, Memory<byte> memory)
        {
            Debug.Assert(m_checkpointWriter != null);
            Debug.Assert(m_existingPages != null);
            if (!m_existingPages.Contains(pageId))
            {
                m_existingPages.Add(pageId);
                m_listUpdated = true;
            }
            return m_checkpointWriter.WritePageMemory(pageId, memory);
        }
    }
}
