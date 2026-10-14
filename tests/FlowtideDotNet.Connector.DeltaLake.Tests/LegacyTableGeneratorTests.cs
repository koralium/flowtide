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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class LegacyTableGeneratorTests
    {
        [Fact]
        public async Task GeneratorRootReadsBackThroughReadTable()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(GeneratorRootReadsBackThroughReadTable)}");
            var root = await LegacyTableGenerator.Create(storage, "t", 20_000, 2, 6, withDvs: true);
            var expected = root.Where(x => x.Add != null).ToDictionary(x => Key(x.Add!), x => x.Add!);

            // Version 0 reads the checkpoint the generator wrote
            var atRoot = (await DeltaTransactionReader.ReadTable(storage, "t", maxVersion: 0))!;
            Assert.Equal(0, atRoot.Version);
            Assert.Equal(0, atRoot.StartCheckpointVersion);
            Assert.Equal(20_000, atRoot.AddFiles.Count);
            Assert.Contains(atRoot.AddFiles, x => x.DeletionVector != null);
            foreach (var add in atRoot.AddFiles)
            {
                Assert.True(expected.TryGetValue(Key(add), out var written), add.Path);
                Assert.Equal(written.Statistics, add.Statistics);
                Assert.Equal(written.Size, add.Size);
                Assert.Equal(written.ModificationTime, add.ModificationTime);
            }

            // Every tail commit replaces one live file or its deletion vector
            var head = (await DeltaTransactionReader.ReadTable(storage, "t"))!;
            Assert.Equal(6, head.Version);
            Assert.Equal(0, head.StartCheckpointVersion);
            Assert.Equal(20_000, head.AddFiles.Count);
        }

        private static string Key(DeltaAddAction add) => $"{add.Path}|{add.DeletionVector?.UniqueId}";
    }
}
