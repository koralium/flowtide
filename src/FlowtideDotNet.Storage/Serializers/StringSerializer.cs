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

using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Storage.Serializers
{
    public class StringSerializer : IBplusTreeSerializer<string>
    {
        public Task CheckpointAsync()
        {
            return Task.CompletedTask;
        }

        public void Deserialize(in BinaryReader reader, in List<string> values)
        {
            var count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                values.Add(reader.ReadString());
            }
        }

        public void Serialize(in BinaryWriter writer, in List<string> values)
        {
            writer.Write(values.Count);
            foreach (var value in values)
            {
                writer.Write(value);
            }
        }
    }
}
