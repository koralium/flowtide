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

using BenchmarkDotNet.Attributes;

namespace FlowtideDotNet.Benchmarks.Stream
{
    /// <summary>
    /// Initial load benchmarks for the bulk window operator, one per frame family.
    /// </summary>
    [Config(typeof(BenchmarkConfiguration))]
    public class WindowBenchmark
    {
        private int iterationId = 0;
        private BenchmarkTestStream? _stream;

        [IterationSetup]
        public void IterationSetup()
        {
            _stream = new BenchmarkTestStream("window" + iterationId.ToString());
            _stream.SourceImmutable();
            _stream.Generate(500_000);
            _stream.CachePageCount = 200_000;
            iterationId++;
        }

        [Benchmark]
        public async Task WindowRowNumberSinglePartition()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT UserKey, ROW_NUMBER() OVER (ORDER BY UserKey) FROM users
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(WindowRowNumberSinglePartition))]
        public void AfterWindowRowNumberSinglePartition()
        {
            StreamGraphMetadata.SaveGraphData(nameof(WindowRowNumberSinglePartition), _stream!.GetDiagnosticsGraph());
        }

        [Benchmark]
        public async Task WindowRunningSumSinglePartition()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT UserKey, SUM(DoubleValue) OVER (ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM users
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(WindowRunningSumSinglePartition))]
        public void AfterWindowRunningSumSinglePartition()
        {
            StreamGraphMetadata.SaveGraphData(nameof(WindowRunningSumSinglePartition), _stream!.GetDiagnosticsGraph());
        }

        [Benchmark]
        public async Task WindowRowNumber()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT UserKey, ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) FROM users
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(WindowRowNumber))]
        public void AfterWindowRowNumber()
        {
            StreamGraphMetadata.SaveGraphData(nameof(WindowRowNumber), _stream!.GetDiagnosticsGraph());
        }

        [Benchmark]
        public async Task WindowRunningSum()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT UserKey, SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM users
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(WindowRunningSum))]
        public void AfterWindowRunningSum()
        {
            StreamGraphMetadata.SaveGraphData(nameof(WindowRunningSum), _stream!.GetDiagnosticsGraph());
        }

        [Benchmark]
        public async Task WindowBoundedSum()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT UserKey, SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) FROM users
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(WindowBoundedSum))]
        public void AfterWindowBoundedSum()
        {
            StreamGraphMetadata.SaveGraphData(nameof(WindowBoundedSum), _stream!.GetDiagnosticsGraph());
        }

        [Benchmark]
        public async Task WindowSumFollowing()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT UserKey, SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) FROM users
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(WindowSumFollowing))]
        public void AfterWindowSumFollowing()
        {
            StreamGraphMetadata.SaveGraphData(nameof(WindowSumFollowing), _stream!.GetDiagnosticsGraph());
        }

        [Benchmark]
        public async Task WindowUnboundedSum()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT UserKey, SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM users
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(WindowUnboundedSum))]
        public void AfterWindowUnboundedSum()
        {
            StreamGraphMetadata.SaveGraphData(nameof(WindowUnboundedSum), _stream!.GetDiagnosticsGraph());
        }

        [Benchmark]
        public async Task WindowMinBy365()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT UserKey, min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) FROM users
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(WindowMinBy365))]
        public void AfterWindowMinBy365()
        {
            StreamGraphMetadata.SaveGraphData(nameof(WindowMinBy365), _stream!.GetDiagnosticsGraph());
        }
    }

    /// <summary>
    /// Update path benchmarks for the bulk window operator, the stream is started with initial data in
    /// the setup and the benchmark measures appending rows through a watermark cycle.
    /// </summary>
    [Config(typeof(BenchmarkConfiguration))]
    public class WindowAppendBenchmark
    {
        private const string RunningSumQuery = @"
            INSERT INTO output
            SELECT UserKey, SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM users";

        private const string MinBy365Query = @"
            INSERT INTO output
            SELECT UserKey, min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) FROM users";

        private int iterationId = 0;
        private BenchmarkTestStream? _stream;

        private void SetupStream(string sql)
        {
            _stream = new BenchmarkTestStream("windowappend" + iterationId.ToString());
            _stream.Generate(100_000);
            _stream.CachePageCount = 100_000;
            iterationId++;
            _stream.StartStream(sql, 1).GetAwaiter().GetResult();
            _stream.WaitForUpdate().GetAwaiter().GetResult();
        }

        [IterationSetup(Target = nameof(WindowRunningSumAppend))]
        public void SetupRunningSumAppend()
        {
            SetupStream(RunningSumQuery);
        }

        [Benchmark]
        public async Task WindowRunningSumAppend()
        {
            _stream!.Generate(50_000);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(WindowRunningSumAppend))]
        public void AfterWindowRunningSumAppend()
        {
            StreamGraphMetadata.SaveGraphData(nameof(WindowRunningSumAppend), _stream!.GetDiagnosticsGraph());
        }

        [IterationSetup(Target = nameof(WindowMinBy365Append))]
        public void SetupMinBy365Append()
        {
            SetupStream(MinBy365Query);
        }

        [Benchmark]
        public async Task WindowMinBy365Append()
        {
            _stream!.Generate(50_000);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(WindowMinBy365Append))]
        public void AfterWindowMinBy365Append()
        {
            StreamGraphMetadata.SaveGraphData(nameof(WindowMinBy365Append), _stream!.GetDiagnosticsGraph());
        }
    }
}
