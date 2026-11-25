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

using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using System.Reflection;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class HostedSinkChunkRetryTests
{
    private static Task Invoke(HostedSinkOperatorTests fixture, string method, params object[] arguments)
    {
        return (Task)typeof(HostedSinkOperatorTests).GetMethod(method, BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(fixture, arguments)!;
    }

    private static DifferentialDestination Destination(HostedSinkOperatorTests fixture, string field)
    {
        return (DifferentialDestination)typeof(HostedSinkOperatorTests).GetField(field, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(fixture)!;
    }

    [Fact]
    public async Task KeysThatDoNotFitTheRestOfAChunkAreWrittenAsThePlainSinkWritesThem()
    {
        using var fixture = new HostedSinkOperatorTests();
        await Invoke(fixture, "Start");
        var hosted = Destination(fixture, "_hostedDestination");
        var plain = Destination(fixture, "_plainDestination");
        int maximumRows = 0;
        int sendsWithRows = 0;
        hosted.ObserveBatch = batch =>
        {
            if (batch.Columns[0] is not FlowtideDotNet.Core.ColumnStore.AlwaysNullColumn)
            {
                maximumRows = Math.Max(maximumRows, batch.Count);
            }
        };

        var rows = new List<(long id, long key, long number, string text)>();
        long id = 0;
        for (long key = 0; key < 1200; key++)
        {
            int count = 1 + (int)(key % 6);
            for (int j = 0; j < count; j++)
            {
                rows.Add((id++, key, j, $"t{(j * 7 + key) % count}-{key}-{j}"));
            }
        }
        await Invoke(fixture, "Store", rows.ToArray());

        await Invoke(fixture, "Send", rows.Select(x => (1, x.key, x.number, x.text)).ToArray());
        Assert.Equal(1200, plain.Rows().Count);
        Assert.Equal(plain.Operations, hosted.Operations);
        sendsWithRows = hosted.Operations.Count;

        var retract = new List<(int, long, long, string)>();
        foreach (var group in rows.GroupBy(x => x.key))
        {
            if (group.Key % 3 == 0)
            {
                retract.AddRange(group.Select(x => (-1, x.key, x.number, x.text)));
            }
            else if (group.Key % 3 == 1)
            {
                var lowest = group.OrderBy(x => x.text, StringComparer.Ordinal).First();
                retract.Add((-1, lowest.key, lowest.number, lowest.text));
            }
        }
        await Invoke(fixture, "Send", retract.ToArray());

        Assert.Equal(800, plain.Rows().Count);
        Assert.Equal(plain.Operations, hosted.Operations);
        Assert.InRange(maximumRows, 1, 1024);
        Assert.True(hosted.Operations.Count > sendsWithRows);
    }
}
