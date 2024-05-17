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

using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using FlowtideDotNet.Base.Metrics;
using Perfolizer.Horology;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Benchmarks.Stream
{
    internal class ProcessedEventsColumn : IColumn
    {
        public string Id => nameof(ProcessedEventsColumn);

        public string ColumnName => "Processed Events / s";
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 1;
        public bool IsNumeric => true;
        public UnitType UnitType { get; private set; }
        public string Legend => "Custom Column";
        public bool IsAvailable(Summary summary) => true;
        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => true;

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            string benchmarkName = benchmarkCase.Descriptor.WorkloadMethod.Name;
            var streamGraph = StreamGraphMetadata.LoadGraphData(benchmarkName);

            var sum = streamGraph!.Nodes.Select(x =>
            {
                var counter = x.Value.Counters.FirstOrDefault(x => x.Name == "flowtide_events_processed");
                if (counter == null)
                {
                    return 0;
                }
                return counter.Total.Sum;
            }).Sum();
            var meanValue = TimeInterval.FromNanoseconds(summary[benchmarkCase]!.ResultStatistics!.Mean).ToSeconds();
            summary.GetColumns().FirstOrDefault(x => x.ColumnName == "Mean");
            var perSecond = Math.Round(sum / (decimal)meanValue);
            return perSecond.ToString();
        }

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            return GetValue(summary, benchmarkCase);
        }
    }
}
