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

using FlowtideDotNet.Core.Operators.StatefulBinary;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Union
{
    public class UnionOperator : StatefulBinaryOperator
    {
        public override string DisplayName => "Union";

        public UnionOperator(ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
        }

        private int WeightFunction(int weight1, int weight2)
        {
            return Math.Max(weight1, weight2);
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnInput1(StreamEventBatch streamEventBatch, long time)
        {
            List<StreamEvent> output = new List<StreamEvent>();
            foreach(var e in streamEventBatch.Events)
            {
                var weights = Input1Storage.UpsertAndGetWeights(e, e.Weight);
                var otherWeight = Input2Storage.GetWeights(e);

                var oldWeight = WeightFunction(weights.previousWeight, otherWeight);
                var newWeight = WeightFunction(weights.newWeight, otherWeight);

                if (oldWeight != newWeight)
                {
                    output.Add(new StreamEvent(newWeight - oldWeight, 0, e.Memory));
                }
            }

            if (output.Count > 0)
            {
                yield return new StreamEventBatch(streamEventBatch.Schema, output);
            }
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnInput2(StreamEventBatch streamEventBatch, long time)
        {
            List<StreamEvent> output = new List<StreamEvent>();
            foreach (var e in streamEventBatch.Events)
            {
                var weights = Input2Storage.UpsertAndGetWeights(e, e.Weight);
                var otherWeight = Input1Storage.GetWeights(e);

                var oldWeight = WeightFunction(weights.previousWeight, otherWeight);
                var newWeight = WeightFunction(weights.newWeight, otherWeight);

                if (oldWeight != newWeight)
                {
                    output.Add(new StreamEvent(newWeight - oldWeight, 0, e.Memory));
                }
            }

            if (output.Count > 0)
            {
                yield return new StreamEventBatch(streamEventBatch.Schema, output);
            }
        }

        public override Task DeleteAsync()
        {
            throw new NotImplementedException();
        }
    }
}
