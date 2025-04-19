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
using FlowtideDotNet.Substrait.Type;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Files.Internal
{
    internal static class StringToDataTypeConverter
    {
        public static Action<string?, IColumn> GetConvertFunction(SubstraitBaseType type)
        {
            switch (type.Type)
            {
                case SubstraitType.Any:
                    return (s, c) =>
                    {
                        if (s == null)
                        {
                            c.Add(NullValue.Instance);
                        }
                        else
                        {
                            c.Add(new StringValue(s));
                        }
                    };
                case SubstraitType.Int64:
                case SubstraitType.Int32:
                    return (s, c) =>
                    {
                        if (s == null)
                        {
                            c.Add(NullValue.Instance);
                        }
                        else
                        {
                            if (long.TryParse(s, out long i))
                            {
                                c.Add(new Int64Value(i));
                            }
                            else
                            {
                                c.Add(NullValue.Instance);
                            }
                        }
                    };
                case SubstraitType.Struct:
                    throw new NotSupportedException("Struct type is not supported in CSV output");
                case SubstraitType.List:
                    throw new NotSupportedException("List type is not supported in CSV output");
                case SubstraitType.String:
                    return (s, c) =>
                    {
                        if (s == null)
                        {
                            c.Add(NullValue.Instance);
                        }
                        else
                        {
                            c.Add(new StringValue(s));
                        }
                    };
                case SubstraitType.Bool:
                    return (s, c) =>
                    {
                        if (s == null)
                        {
                            c.Add(NullValue.Instance);
                        }
                        else
                        {
                            if (bool.TryParse(s, out bool b))
                            {
                                c.Add(new BoolValue(b));
                            }
                            else
                            {
                                c.Add(NullValue.Instance);
                            }
                        }
                    };
                case SubstraitType.Decimal:
                    return (s, c) =>
                    {
                        if (s == null)
                        {
                            c.Add(NullValue.Instance);
                        }
                        else
                        {
                            if (decimal.TryParse(s, CultureInfo.InvariantCulture, out decimal d))
                            {
                                c.Add(new DecimalValue(d));
                            }
                            else
                            {
                                c.Add(NullValue.Instance);
                            }
                        }
                    };
                case SubstraitType.Binary:
                    return (s, c) =>
                    {
                        if (s == null)
                        {
                            c.Add(NullValue.Instance);
                        }
                        else
                        {
                            c.Add(new BinaryValue(Encoding.UTF8.GetBytes(s)));
                        }
                    };
                case SubstraitType.Date:
                    return (s, c) =>
                    {
                        if (s == null)
                        {
                            c.Add(NullValue.Instance);
                        }
                        else
                        {
                            if (DateTime.TryParse(s, out DateTime date))
                            {
                                c.Add(new TimestampTzValue(date));
                            }
                            else
                            {
                                c.Add(NullValue.Instance);
                            }
                        }
                    };
                case SubstraitType.Fp32:
                case SubstraitType.Fp64:
                    return (s, c) =>
                    {
                        if (s == null)
                        {
                            c.Add(NullValue.Instance);
                        }
                        else
                        {
                            if (double.TryParse(s, out double d))
                            {
                                c.Add(new DoubleValue(d));
                            }
                            else
                            {
                                c.Add(NullValue.Instance);
                            }
                        }
                    };
                case SubstraitType.TimestampTz:
                    return (s, c) =>
                    {
                        if (s == null)
                        {
                            c.Add(NullValue.Instance);
                        }
                        else
                        {
                            if (DateTimeOffset.TryParse(s, out DateTimeOffset date))
                            {
                                c.Add(new TimestampTzValue(date));
                            }
                            else
                            {
                                c.Add(NullValue.Instance);
                            }
                        }
                    };
                default:
                    throw new NotSupportedException($"Type {type.Type} is not supported in CSV output");
            }
        }
    }
}
