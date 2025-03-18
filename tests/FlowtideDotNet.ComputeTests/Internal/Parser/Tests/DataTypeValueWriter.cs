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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using System.Globalization;

namespace FlowtideDotNet.ComputeTests.SourceGenerator.Internal.Tests
{
    internal static class DataTypeValueWriter
    {
        public static IDataValue WriteDataValue(string value, string dataType)
        {
            if (value == "Null")
            {
                return NullValue.Instance;
            }
            switch (dataType)
            {
                case "i8":
                case "i16":
                case "i32":
                case "i64":
                    return new Int64Value(long.Parse(value, CultureInfo.InvariantCulture));
                case "fp32":
                case "fp64":
                    if (value == "inf")
                    {
                        return new DoubleValue(double.PositiveInfinity);
                    }
                    else if (value == "-inf")
                    {
                        return new DoubleValue(double.NegativeInfinity);
                    }
                    else if (value == "nan")
                    {
                        return new DoubleValue(double.NaN);
                    }
                    return new DoubleValue(double.Parse(value, CultureInfo.InvariantCulture));
                case "str":
                    value = value.Trim('\'');
                    return new StringValue(value);
                case "dec":
                    return new DecimalValue(decimal.Parse(value, CultureInfo.InvariantCulture));
                case "ts":
                    value = value.Trim('\'');
                    if (DateTimeOffset.TryParse(value, out var result))
                    {
                        return new TimestampTzValue(result);
                    }
                    throw new Exception($"Could not parse timestamp {value}");

            }
            throw new NotImplementedException(dataType);
        }
    }
}
