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


using PromQL.Parser;

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class BinaryExecutor : IMetricExecutor
    {
        private readonly IMetricExecutor left;
        private readonly IMetricExecutor right;
        private readonly Operators.Binary op;

        public BinaryExecutor(IMetricExecutor left, IMetricExecutor right, Operators.Binary op)
        {
            this.left = left;
            this.right = right;
            this.op = op;
        }

        public IReadOnlyDictionary<string, string> Tags => left.Tags;

        public SerieType SerieType => GetSerieType();

        private string GetName()
        {
            switch (op)
            {
                case Operators.Binary.Lss:
                    return $"{left.Name} < {right.Name}";
                case Operators.Binary.Lte:
                    return $"{left.Name} <= {right.Name}";
                case Operators.Binary.Eql:
                    return $"{left.Name} == {right.Name}";
                case Operators.Binary.Neq:
                    return $"{left.Name} != {right.Name}";
                case Operators.Binary.Gte:
                    return $"{left.Name} >= {right.Name}";
                case Operators.Binary.Gtr:
                    return $"{left.Name} > {right.Name}";
                case Operators.Binary.And:
                    return $"{left.Name} AND {right.Name}";
                case Operators.Binary.Or:
                    return $"{left.Name} OR {right.Name}";
                case Operators.Binary.Add:
                    return $"{left.Name} + {right.Name}";
                case Operators.Binary.Sub:
                    return $"{left.Name} - {right.Name}";
                case Operators.Binary.Mul:
                    return $"{left.Name} * {right.Name}";
                case Operators.Binary.Div:
                    return $"{left.Name} / {right.Name}";
                case Operators.Binary.Mod:
                    return $"{left.Name} % {right.Name}";
            }
            return $"{left.Name} {op} {right.Name}";
        }

        public string Name => GetName();

        private SerieType GetSerieType()
        {
            if (left.SerieType == right.SerieType)
            {
                return left.SerieType;
            }
            if (left.SerieType == SerieType.Matrix || right.SerieType == SerieType.Matrix)
            {
                return SerieType.Matrix;
            }
            return SerieType.Scalar;
        }

        private (double, bool) GetValue(double valueLeft, double valueRight, Operators.Binary op)
        {
            switch (op)
            {
                case Operators.Binary.Add:
                    return (valueLeft + valueRight, true);
                case Operators.Binary.Sub:
                    return (valueLeft - valueRight, true);
                case Operators.Binary.Mul:
                    return (valueLeft * valueRight, true);
                case Operators.Binary.Div:
                    if (valueRight == 0)
                        return (double.NaN, true);
                    return (valueLeft / valueRight, true);
                case Operators.Binary.Mod:
                    return (valueLeft % valueRight, true);
                case Operators.Binary.Gtr:
                    if (valueLeft > valueRight)
                        return (valueLeft, true);
                    return (double.NaN, false);
                case Operators.Binary.Gte:
                    if (valueLeft >= valueRight)
                        return (valueLeft, true);
                    return (double.NaN, false);
                default:
                    return (double.NaN, true);
            }
        }

        private async IAsyncEnumerable<MetricResult> GetValuesRightScalar(long startTimestamp, long endTimestamp, int stepWidth)
        {
            var leftIterator = left.GetValues(startTimestamp, endTimestamp, stepWidth).GetAsyncEnumerator();
            var rightIterator = right.GetValues(startTimestamp, endTimestamp, stepWidth).GetAsyncEnumerator();

            await rightIterator.MoveNextAsync();
            var rightValue = rightIterator.Current.value;

            while (await leftIterator.MoveNextAsync())
            {
                var value = GetValue(leftIterator.Current.value, rightValue, op);
                if (value.Item2)
                {
                    yield return new MetricResult(value.Item1, leftIterator.Current.timestamp);
                }
            }
        }

        private async IAsyncEnumerable<MetricResult> GetValuesRightBothMatrix(long startTimestamp, long endTimestamp, int stepWidth)
        {
            var leftIterator = left.GetValues(startTimestamp, endTimestamp, stepWidth).GetAsyncEnumerator();
            var rightIterator = right.GetValues(startTimestamp, endTimestamp, stepWidth).GetAsyncEnumerator();

            // Loop left and right until both have the same timestamp and then return a value
            // If left is behind, loop left until they match, same for right, if one of them ends, end the loop completely

            bool leftAvailable = await leftIterator.MoveNextAsync();
            bool rightAvailable = await rightIterator.MoveNextAsync();

            while (leftAvailable && rightAvailable)
            {
                if (leftIterator.Current.timestamp == rightIterator.Current.timestamp)
                {
                    var value = GetValue(leftIterator.Current.value, rightIterator.Current.value, op);
                    if (value.Item2)
                    {
                        yield return new MetricResult(value.Item1, leftIterator.Current.timestamp);
                    }
                    leftAvailable = await leftIterator.MoveNextAsync();
                    rightAvailable = await rightIterator.MoveNextAsync();
                }
                else if (leftIterator.Current.timestamp < rightIterator.Current.timestamp)
                {
                    while (leftIterator.Current.timestamp < rightIterator.Current.timestamp && leftAvailable)
                    {
                        leftAvailable = await leftIterator.MoveNextAsync();
                    }
                }
                else
                {
                    while (rightIterator.Current.timestamp < leftIterator.Current.timestamp && rightAvailable)
                    {
                        rightAvailable = await rightIterator.MoveNextAsync();
                    }
                }
            }
        }

        public IAsyncEnumerable<MetricResult> GetValues(long startTimestamp, long endTimestamp, int stepWidth)
        {
            if (right.SerieType == SerieType.Scalar)
            {
                return GetValuesRightScalar(startTimestamp, endTimestamp, stepWidth);
            }
            return GetValuesRightBothMatrix(startTimestamp, endTimestamp, stepWidth);
        }
    }
}
