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

using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class AutoFileCacheStorageTests
{
    private static Plan Chain()
    {
        Relation joined = Read("a", false);
        foreach (var table in new[] { "b", "c", "d", "e" })
        {
            joined = new MergeJoinRelation()
            {
                Left = joined,
                Right = Read(table, false),
                Type = JoinType.Left,
                LeftKeys = [Field(0)],
                RightKeys = [Field(joined.OutputLength + 1)]
            };
        }
        return Write(joined, 20);
    }

    private static DifferentialTable[] ChainTables()
    {
        return
        [
            Table("a", Row(1, 10, "a1", "x"), Row(2, 20, "a2", null), Row(3, 30, null, "z")),
            Table("b", Row(10, 1, "b1", "p"), Row(11, 2, "b2", "q")),
            Table("c", Row(20, 1, "c1", null), Row(21, 3, "c3", "r")),
            Table("d", Row(30, 2, "d2", "s")),
            Table("e", Row(40, 1, "e1", "t"), Row(41, 1, "e1b", null))
        ];
    }

    [Theory]
    [InlineData(LateMaterializationMode.Off)]
    [InlineData(LateMaterializationMode.Auto)]
    public async Task AStreamOnFileCacheStorageRunsUnderTheDefaultMode(LateMaterializationMode mode)
    {
        var directory = Path.Combine(Path.GetTempPath(), "flowtide-file-cache-a-" + Guid.NewGuid().ToString("N"));
        using var storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = directory });
        await using var stream = new DifferentialStream(Chain, builder => builder
            .SetLateMaterialization(mode)
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage }), ChainTables());
        await stream.StartAsync();
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (stream.Completed == 0 && stream.Failures.IsEmpty && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        Assert.True(stream.Failures.IsEmpty, string.Join(Environment.NewLine, stream.Failures.Select(x => x?.GetType().Name + ": " + x?.Message)));
        Assert.True(stream.Completed > 0);
        Assert.NotEmpty(stream.Result());
    }
}
