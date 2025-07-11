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

using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal class ExchangeContainer
    {
        public ExchangeContainer(EmitData emitData, int relationId, int outputLength, ExchangeRelation exchangeRelation, string? subStreamName)
        {
            EmitData = emitData;
            RelationId = relationId;
            OutputLength = outputLength;
            ExchangeRelation = exchangeRelation;
            SubStreamName = subStreamName;
        }

        public int RelationId { get; }

        public EmitData EmitData { get; }

        public int OutputLength { get; }

        public ExchangeRelation ExchangeRelation { get; }

        public string? SubStreamName { get; }
    }
}
