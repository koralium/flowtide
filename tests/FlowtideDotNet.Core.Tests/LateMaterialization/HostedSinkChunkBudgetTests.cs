using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using System.Reflection;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class HostedSinkChunkBudgetTests
    {
        private static Task Invoke(HostedSinkOperatorTests fixture, string method, params object[] arguments)
        {
            return (Task)typeof(HostedSinkOperatorTests).GetMethod(method, BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(fixture, arguments)!;
        }

        private static DifferentialDestination Destination(HostedSinkOperatorTests fixture)
        {
            return (DifferentialDestination)typeof(HostedSinkOperatorTests).GetField("_hostedDestination", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(fixture)!;
        }

        [Fact]
        public async Task AdjacentKeysCannotExpandAFetchChunkPastItsRowLimit()
        {
            using var fixture = new HostedSinkOperatorTests();
            await Invoke(fixture, "Start");
            await Invoke(fixture, "Store", new[] { (0L, 0L, 0L, "warmup") });
            await Invoke(fixture, "Send", new[] { (1, 0L, 0L, "warmup") });
            int maximumRows = 0;
            Destination(fixture).ObserveBatch = batch => maximumRows = Math.Max(maximumRows, batch.Count);
            var rows = Enumerable.Range(1, 1600).Select(x => ((long)x, x <= 800 ? 7L : 8L, (long)x, "value " + x)).ToArray();
            await Invoke(fixture, "Store", rows);
            await Invoke(fixture, "Send", rows.Select(x => (1, x.Item2, x.Item3, x.Item4)).ToArray());

            Assert.InRange(maximumRows, 1, 1024);
        }

        [Fact]
        public async Task OneKeyCannotExpandAFetchChunkPastItsRowLimit()
        {
            using var fixture = new HostedSinkOperatorTests();
            await Invoke(fixture, "Start");
            int maximumRows = 0;
            Destination(fixture).ObserveBatch = batch => maximumRows = Math.Max(maximumRows, batch.Count);
            var rows = Enumerable.Range(0, 1025).Select(x => ((long)x, 7L, (long)x, "value " + x)).ToArray();
            await Invoke(fixture, "Store", rows);
            await Invoke(fixture, "Send", rows.Select(x => (1, x.Item2, x.Item3, x.Item4)).ToArray());

            Assert.InRange(maximumRows, 1, 1024);
        }

        [Fact]
        public async Task SmallKeysAndThenAFullKeyCannotExpandAFetchChunkPastItsRowLimit()
        {
            using var fixture = new HostedSinkOperatorTests();
            await Invoke(fixture, "Start");
            int maximumRows = 0;
            Destination(fixture).ObserveBatch = batch => maximumRows = Math.Max(maximumRows, batch.Count);
            var single = Enumerable.Range(0, 1023).Select(x => ((long)x, (long)x, (long)x, "single " + x)).ToArray();
            var full = Enumerable.Range(0, 1024).Select(x => ((long)(10000 + x), 5000L, (long)x, "full " + x)).ToArray();
            var rows = single.Concat(full).ToArray();
            await Invoke(fixture, "Store", rows);
            await Invoke(fixture, "Send", rows.Select(x => (1, x.Item2, x.Item3, x.Item4)).ToArray());

            Assert.InRange(maximumRows, 1, 1024);
        }

        [Fact]
        public async Task WideRowsCannotExpandAFetchChunkPastItsByteLimit()
        {
            using var fixture = new HostedSinkOperatorTests();
            await Invoke(fixture, "Start");
            long maximumFetchedBytes = 0;
            Destination(fixture).ObserveBatch = batch => maximumFetchedBytes = Math.Max(maximumFetchedBytes, batch.Columns[1].GetByteSize());
            var rows = Enumerable.Range(0, 129).Select(x => ((long)x, (long)x, (long)x, new string((char)('a' + x % 26), 65536))).ToArray();
            await Invoke(fixture, "Store", rows);
            await Invoke(fixture, "Send", rows.Select(x => (1, x.Item2, x.Item3, x.Item4)).ToArray());

            Assert.InRange(maximumFetchedBytes, 1, 8 * 1024 * 1024);
        }
    }
}
