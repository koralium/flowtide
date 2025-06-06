﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.Compute.Internal
{
    public class ColumnFunctionDefinition
    {
        public ColumnFunctionDefinition(
            string uri,
            string name,
            Func<ScalarFunction, ColumnParameterInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ColumnParameterInfo>, IFunctionServices, System.Linq.Expressions.Expression> mapFunc)
        {
            Uri = uri;
            Name = name;
            MapFunc = mapFunc;
        }

        public string Uri { get; }

        public string Name { get; }

        public Func<ScalarFunction, ColumnParameterInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ColumnParameterInfo>, IFunctionServices, System.Linq.Expressions.Expression> MapFunc { get; }
    }

    public class FunctionDefinition
    {
        public FunctionDefinition(
            string uri,
            string name,
            Func<ScalarFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, System.Linq.Expressions.Expression> mapFunc)
        {
            Uri = uri;
            Name = name;
            MapFunc = mapFunc;
        }

        public string Uri { get; }

        public string Name { get; }

        public Func<ScalarFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, System.Linq.Expressions.Expression> MapFunc { get; }
    }
}
