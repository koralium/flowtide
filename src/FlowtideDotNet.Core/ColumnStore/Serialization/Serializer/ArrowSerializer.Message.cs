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

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
#pragma warning disable CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    internal ref partial struct ArrowSerializer
#pragma warning restore CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    {
        public int CreateMessage(
            short version = 0,
            MessageHeader headerType = 0,
            int headerOffset = 0,
            int bodyLength = 0,
            int custom_metadataOffset = 0)
        {
            StartTable(5);
            AddBodyLength(bodyLength);
            AddCustomMetadata(custom_metadataOffset);
            AddHeader(headerOffset);
            AddVersion(version);
            AddHeaderType(headerType);
            return EndTable();
        }

        void AddCustomMetadata(int customMetadataOffset)
        {
            AddOffset(4, customMetadataOffset, 0);
        }

        void AddBodyLength(long bodyLength)
        {
            AddLong(3, bodyLength, 0);
        }

        void AddHeader(int headerOffset)
        {
            AddOffset(2, headerOffset, 0);
        }

        void AddVersion(short version)
        {
            AddShort(0, version, 0);
        }

        void AddHeaderType(MessageHeader headerType)
        {
            AddByte(1, (byte)headerType, 0);
        }
    }
}
