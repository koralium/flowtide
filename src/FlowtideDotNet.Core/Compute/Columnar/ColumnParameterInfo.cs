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

using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    public class ColumnParameterInfo
    {
        public ColumnParameterInfo(IReadOnlyList<ParameterExpression> batchParameters, IReadOnlyList<ParameterExpression> indexParameters, IReadOnlyList<int> relativeIndices, Expression resultDataValue)
        {
            BatchParameters = batchParameters;
            IndexParameters = indexParameters;
            RelativeIndices = relativeIndices;
            ResultDataValue = resultDataValue;
        }

        public IReadOnlyList<ParameterExpression> BatchParameters { get; }
        public IReadOnlyList<ParameterExpression> IndexParameters { get; }

        /// <summary>
        /// Parameters and relative indices must be in sorted order
        /// </summary>
        public IReadOnlyList<int> RelativeIndices { get; }

        public Expression ResultDataValue { get; }

        public ColumnParameterInfo UpdateResultDataValue(Expression expression)
        {
            return new ColumnParameterInfo(BatchParameters, IndexParameters, RelativeIndices, expression);
        }
    }
}
