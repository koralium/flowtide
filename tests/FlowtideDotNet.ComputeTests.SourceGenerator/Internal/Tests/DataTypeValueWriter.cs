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

using System;
using System.Collections.Generic;
using System.Text;

namespace FlowtideDotNet.ComputeTests.SourceGenerator.Internal.Tests
{
    internal static class DataTypeValueWriter
    {
        public static string WriteDataValue(string value, string dataType)
        {
            if (value == "Null")
            {
                return "NullValue.Instance";
            }
            switch (dataType)
            {
                case "i8":
                case "i16":
                case "i32":
                case "i64":
                    return $"new Int64Value({value})";
                case "fp32":
                case "fp64":
                    if (value == "inf")
                    {
                        return "new DoubleValue(double.PositiveInfinity)";
                    }
                    else if (value == "-inf")
                    {
                        return "new DoubleValue(double.NegativeInfinity)";
                    }
                    return $"new DoubleValue({value})";
                case "str":
                    return $"new StringValue(\"{value}\")";
            }
            throw new NotImplementedException(dataType);
        }
    }
}
