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

using FlowtideDotNet.AspNetCore.Internal.TimeSeries.Executors;
using PromQL.Parser;
using PromQL.Parser.Ast;

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class PromQLVisitor : IVisitor
    {
        private struct GroupingContainer
        {
            public Dictionary<string, string> tags;
            public List<IMetricExecutor> series;

            public GroupingContainer(Dictionary<string, string> tags, List<IMetricExecutor> series)
            {
                this.tags = tags;
                this.series = series;
            }
        }
        private struct VisitResult
        {
            public IEnumerable<IMetricExecutor> series;
            public long? duration;

            public VisitResult(IEnumerable<IMetricExecutor> series, long? duration = default)
            {
                this.series = series;
                this.duration = duration;
            }
        }

        private readonly MetricSeries metricSeries;
        private Stack<VisitResult> _callStack;

        public PromQLVisitor(MetricSeries series)
        {
            this.metricSeries = series;
            _callStack = new Stack<VisitResult>();
        }

        public IEnumerable<IMetricExecutor> GetResult()
        {
            return _callStack.Pop().series;
        }

        public void Visit(StringLiteral expr)
        {
            throw new NotImplementedException();
        }

        public void Visit(SubqueryExpr sq)
        {
            throw new NotImplementedException();
        }

        public void Visit(Duration d)
        {
            throw new NotImplementedException();
        }

        public void Visit(NumberLiteral n)
        {
            _callStack.Push(new VisitResult([new ConstantValueExecutor((double)n.Value)]));
        }

        public void Visit(MetricIdentifier mi)
        {
            throw new NotImplementedException();
        }

        public void Visit(LabelMatcher expr)
        {
            throw new NotImplementedException();
        }

        public void Visit(UnaryExpr unary)
        {
            throw new NotImplementedException();
        }

        public void Visit(MatrixSelector ms)
        {
            ms.Vector.Accept(this);
            var result = _callStack.Pop();
            _callStack.Push(new VisitResult(result.series, (long)ms.Duration.Value.TotalMilliseconds));
        }

        public void Visit(OffsetExpr offset)
        {
            throw new NotImplementedException();
        }

        public void Visit(ParenExpression paren)
        {
            throw new NotImplementedException();
        }

        public void Visit(FunctionCall fnCall)
        {

            if (fnCall.Function.Name == "rate")
            {
                fnCall.Args[0].Accept(this);
                var series = _callStack.Pop();

                if (!series.duration.HasValue)
                {
                    throw new InvalidOperationException("rate function requires a duration");
                }
                _callStack.Push(new VisitResult(series.series.Select(x => new RateExecutor(x, series.duration.Value))));
            }
            else
            {
                throw new NotImplementedException();
            }

        }

        public void Visit(VectorMatching vm)
        {
            throw new NotImplementedException();
        }

        public void Visit(BinaryExpr expr)
        {
            expr.LeftHandSide.Accept(this);
            var left = _callStack.Pop();
            expr.RightHandSide.Accept(this);
            var right = _callStack.Pop();
            // Group metric series by tags
            Dictionary<string, List<IMetricExecutor>> seriesGroupings = new Dictionary<string, List<IMetricExecutor>>();
            foreach (var serie in left.series)
            {
                var key = string.Join(",", serie.Tags.Select(x => $"{x.Key}={x.Value}"));
                if (!seriesGroupings.TryGetValue(key, out var series))
                {
                    series = new List<IMetricExecutor>();
                    seriesGroupings.Add(key, series);
                }
                series.Add(serie);
            }
            foreach (var serie in right.series)
            {
                if (serie.Tags.Count == 0)
                {
                    foreach (var grouping in seriesGroupings)
                    {
                        grouping.Value.Add(serie);
                    }
                    continue;
                }
                var key = string.Join(",", serie.Tags.Select(x => $"{x.Key}={x.Value}"));
                if (!seriesGroupings.TryGetValue(key, out var series))
                {
                    series = new List<IMetricExecutor>();
                    seriesGroupings.Add(key, series);
                }
                series.Add(serie);
            }
            List<IMetricExecutor> result = new List<IMetricExecutor>();

            foreach (var serie in seriesGroupings)
            {
                if (serie.Value.Count == 2)
                {
                    result.Add(new BinaryExecutor(serie.Value[0], serie.Value[1], expr.Operator));
                }
            }

            _callStack.Push(new VisitResult(result));
        }

        public void Visit(AggregateExpr expr)
        {
            expr.Expr.Accept(this);
            var result = _callStack.Pop();

            Dictionary<string, GroupingContainer>? groups = default;
            if (expr.GroupingLabels.Length > 0)
            {
                groups = new Dictionary<string, GroupingContainer>();
                foreach (var serie in result.series)
                {
                    var key = string.Join(",", expr.GroupingLabels.Select(x => serie.Tags[x]));
                    if (!groups.TryGetValue(key, out var group))
                    {
                        // Create a dictionary that contains the grouping labels and the values from the series
                        Dictionary<string, string> groupTags = new Dictionary<string, string>();

                        foreach (var label in expr.GroupingLabels)
                        {
                            groupTags.Add(label, serie.Tags[label]);
                        }

                        group = new GroupingContainer(groupTags, new List<IMetricExecutor>());
                        groups.Add(key, group);
                    }
                    group.series.Add(serie);
                }
            }
            if (expr.Operator.Name == "sum")
            {
                if (groups != null)
                {
                    _callStack.Push(new VisitResult(groups.Select(x => new SumExecutor(x.Value.series, x.Value.tags))));
                }
                else
                {
                    _callStack.Push(new VisitResult([new SumExecutor(result.series.ToList())]));
                }
            }
            else if (expr.Operator.Name == "avg")
            {
                if (groups != null)
                {
                    _callStack.Push(new VisitResult(groups.Select(x => new AverageExecutor(x.Value.series, x.Value.tags))));
                }
                else
                {
                    _callStack.Push(new VisitResult([new AverageExecutor(result.series.ToList())]));
                }
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private IEnumerable<MetricSerie> FilterSeries(IEnumerable<MetricSerie> series, LabelMatchers matchers)
        {
            foreach (var matcher in matchers.Matchers)
            {
                switch (matcher.Operator)
                {
                    case Operators.LabelMatch.Equal:
                        series = series.Where(x => x.Tags.ContainsKey(matcher.LabelName) && x.Tags[matcher.LabelName] == matcher.Value.Value);
                        break;
                    case Operators.LabelMatch.NotEqual:
                        series = series.Where(x => !x.Tags.ContainsKey(matcher.LabelName) || x.Tags[matcher.LabelName] != matcher.Value.Value);
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
            return series;
        }

        public void Visit(VectorSelector vs)
        {
            IEnumerable<MetricSerie> series;
            if (vs.MetricIdentifier != null)
            {
                var identifier = vs.MetricIdentifier.Value;
                series = metricSeries.GetSeries(identifier);
            }
            else
            {
                series = metricSeries.GetAllSeries();
            }

            if (vs.LabelMatchers != null)
            {
                series = FilterSeries(series, vs.LabelMatchers);
            }
            _callStack.Push(new VisitResult(series));
        }

        public void Visit(LabelMatchers lms)
        {
            throw new NotImplementedException();
        }
    }
}
