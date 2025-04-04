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
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;

namespace FlowtideDotNet.Benchmarks.Stream
{
    public class BenchmarkConfiguration : ManualConfig
    {
        public BenchmarkConfiguration()
        {
            AddJob(Job.Default.WithMaxIterationCount(10).WithMinIterationCount(1));

            AddLogger(ConsoleLogger.Default);
            AddColumn(TargetMethodColumn.Method);
            // Add the processed events column
            AddColumn(new ProcessedEventsColumn());
        }
    }

    [Config(typeof(BenchmarkConfiguration))]
    public class StreamBenchmark
    {
        private int iterationId = 0;
        private BenchmarkTestStream? _stream;
        [IterationSetup]
        public void IterationSetup()
        {
            _stream = new BenchmarkTestStream(iterationId.ToString());
            _stream.Generate(1_00_000);
            _stream.CachePageCount = 100_000;
            iterationId++;
        }

        [Benchmark]
        public async Task InnerJoin()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT u.userkey FROM users u
            INNER JOIN orders o
            ON u.userkey = o.userkey
            INNER JOIN companies c
            ON u.companyid = c.companyid
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(InnerJoin))]
        public void AfterInnerJoin()
        {
            StreamGraphMetadata.SaveGraphData(nameof(InnerJoin), _stream!.GetDiagnosticsGraph());
        }

        [Benchmark]
        public async Task LeftJoin()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT u.userkey FROM users u
            LEFT JOIN orders o
            ON u.userkey = o.userkey
            LEFT JOIN companies c
            ON u.companyid = c.companyid
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(LeftJoin))]
        public void AfterLeftJoin()
        {
            StreamGraphMetadata.SaveGraphData(nameof(LeftJoin), _stream!.GetDiagnosticsGraph());
        }

        /// <summary>
        /// Runs the following graph:
        /// Read[Users] -> Normalization -> Projection -> Write[Output]
        /// 
        /// The main expected cost is the normalization
        /// </summary>
        /// <returns></returns>
        [Benchmark]
        public async Task ProjectionAndNormalization()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT u.userkey FROM users u
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(ProjectionAndNormalization))]
        public void AfterProjectionAndNormalization()
        {
            StreamGraphMetadata.SaveGraphData(nameof(ProjectionAndNormalization), _stream!.GetDiagnosticsGraph());
        }


        /// <summary>
        /// Runs the following graph:
        /// 
        /// Read[Users] -> Normalization -> Aggregation -> Projection -> Write[Output]
        /// </summary>
        /// <returns></returns>
        [Benchmark]
        public async Task SumAggregation()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT sum(u.userkey) FROM users u
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(SumAggregation))]
        public void AfterSumAggregation()
        {
            StreamGraphMetadata.SaveGraphData(nameof(SumAggregation), _stream!.GetDiagnosticsGraph());
        }

        [Benchmark]
        public async Task ListAggWithMapAggregation()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT 
            p.ProjectKey,
            list_agg(map(
              'userkey',
              u.userkey,
              'firstName',
              u.firstName,
              'lastName',
              u.lastName,
              'active',
              u.active
            )) FROM projects p
            LEFT JOIN projectmembers pm
            ON p.ProjectNumber = pm.ProjectNumber AND p.companyid = pm.companyid
            LEFT JOIN users u
            ON pm.userkey = u.userkey
            GROUP BY p.ProjectKey
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(ListAggWithMapAggregation))]
        public void AfterListAggWithMapAggregation()
        {
            StreamGraphMetadata.SaveGraphData(nameof(ListAggWithMapAggregation), _stream!.GetDiagnosticsGraph());
        }

        [Benchmark]
        public async Task WindowSum()
        {
            await _stream!.StartStream(@"
            INSERT INTO output
            SELECT sum(u.doublevalue) OVER(PARTITION BY CompanyId ORDER BY UserKey) FROM users u
            ", 1);
            await _stream.WaitForUpdate();
        }

        [IterationCleanup(Target = nameof(WindowSum))]
        public void AfterWindowSum()
        {
            StreamGraphMetadata.SaveGraphData(nameof(WindowSum), _stream!.GetDiagnosticsGraph());
        }
    }
}
