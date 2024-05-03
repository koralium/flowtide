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

using FlexBuffers;
using FlowtideDotNet.Core.Compute.Internal.StatefulAggregations;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltInStringFunctions
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());
        private static FlxValue TrueValue = FlxValue.FromBytes(FlexBuffer.SingleValue(true));
        private static FlxValue FalseValue = FlxValue.FromBytes(FlexBuffer.SingleValue(false));

        private static System.Linq.Expressions.MethodCallExpression ConcatExpr(System.Linq.Expressions.Expression array)
        {
            MethodInfo? toStringMethod = typeof(FlxValueStringFunctions).GetMethod("Concat", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            Debug.Assert(toStringMethod != null);
            return System.Linq.Expressions.Expression.Call(toStringMethod, array);
        }

        public static void AddStringFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarFunction(FunctionsString.Uri, FunctionsString.Concat,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    List<System.Linq.Expressions.Expression> expressions = new List<System.Linq.Expressions.Expression>();
                    foreach (var expr in scalarFunction.Arguments)
                    {
                        expressions.Add(visitor.Visit(expr, parametersInfo)!);
                    }
                    var array = System.Linq.Expressions.Expression.NewArrayInit(typeof(FlxValue), expressions);
                    return ConcatExpr(array);
                });

            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.Lower, (x) => LowerImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.Upper, (x) => UpperImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.Trim, (x) => TrimImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.LTrim, (x) => LTrimImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.RTrim, (x) => RTrimImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.To_String, (x) => ToStringImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.StartsWith, (x, y) => StartsWithImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.Like, (x, y, z) => LikeImplementation(x, y, z));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.Replace, (x, y, z) => ReplaceImplementation(x, y, z));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.StringBase64Encode, (x) => StringBase64EncodeImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.StringBase64Decode, (x) => StringBase64DecodeImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.CharLength, (x) => CharLengthImplementation(x));

            functionsRegister.RegisterScalarFunction(FunctionsString.Uri, FunctionsString.Substring,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    if (scalarFunction.Arguments.Count < 2)
                    {
                        throw new InvalidOperationException("Substring function must have atleast 2 arguments");
                    }
                    var expr = visitor.Visit(scalarFunction.Arguments[0], parametersInfo)!;
                    var start = visitor.Visit(scalarFunction.Arguments[1], parametersInfo)!;

                    Expression? length = default;
                    if (scalarFunction.Arguments.Count == 3)
                    {
                        length = visitor.Visit(scalarFunction.Arguments[2], parametersInfo)!;
                    }
                    else
                    {
                        length = Expression.Constant(FlxValue.FromBytes(FlexBuffer.SingleValue(-1)));
                    }
                    MethodInfo? toStringMethod = typeof(FlxValueStringFunctions).GetMethod("Substring", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
                    Debug.Assert(toStringMethod != null);
                    return System.Linq.Expressions.Expression.Call(toStringMethod, expr, start, length);
                });

            StringAggAggregation.Register(functionsRegister);
        }

        private static FlxValue ToStringImplementation(in FlxValue val)
        {
            if (val.IsNull)
            {
                return NullValue;
            }
            return FlxValue.FromBytes(FlexBuffer.SingleValue(FlxValueStringFunctions.ToString(val)));
        }

        private static FlxValue LowerImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.ToLower()));
        }

        private static FlxValue UpperImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.ToUpper()));
        }

        private static FlxValue TrimImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.Trim()));
        }

        private static FlxValue LTrimImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.TrimStart()));
        }

        private static FlxValue RTrimImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.TrimEnd()));
        }

        private static FlxValue StartsWithImplementation(in FlxValue val1, in FlxValue val2)
        {
            if (val1.ValueType != FlexBuffers.Type.String || val2.ValueType != FlexBuffers.Type.String)
            {
                return FalseValue;
            }

            if (val1.AsString.StartsWith(val2.AsString))
            {
                return TrueValue;
            }
            return FalseValue;
        }

        private static FlxValue ReplaceImplementation(in FlxValue val, in FlxValue substring, in FlxValue replacement)
        {
            if (val.ValueType != FlexBuffers.Type.String || substring.ValueType != FlexBuffers.Type.String || replacement.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.Replace(substring.AsString, replacement.AsString)));
        }

        private static FlxValue LikeImplementation(in FlxValue val1, in FlxValue val2, in FlxValue escapeChar)
        {
            if (val1.ValueType != FlexBuffers.Type.String || val2.ValueType != FlexBuffers.Type.String)
            {
                return FalseValue;
            }
            char? escapeCharacter = default;
            // Check if escape char is set
            if (escapeChar.ValueType == FlexBuffers.Type.String)
            {
                escapeCharacter = escapeChar.AsString[0];
            }

            var result = IsLike(val1.AsString, val2.AsString, escapeCharacter) ? TrueValue : FalseValue;
            return result;
        }

        private static bool IsLike(string input, string pattern, char? escapeCharacter)
        {
            // Convert SQL LIKE pattern to regex pattern
            string regexPattern = ConvertLikeToRegex(pattern, escapeCharacter);

            // Perform regex match
            return Regex.IsMatch(input, regexPattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(5));
        }

        private static string ConvertLikeToRegex(string pattern, char? escapeCharacter)
        {
            StringBuilder regexPattern = new StringBuilder();
            regexPattern.Append('^'); // Add start anchor
            bool escapeNext = false; // Flag to indicate next character is escaped
            bool inCharSet = false; // Flag to indicate if currently parsing a character set

            for (int i = 0; i < pattern.Length; i++)
            {
                char c = pattern[i];
                if (escapeCharacter.HasValue && c == escapeCharacter && !escapeNext && !inCharSet)
                {
                    escapeNext = true; // Next character is escaped
                    continue;
                }

                if (c == '[' && !escapeNext)
                {
                    inCharSet = true;
                    regexPattern.Append(c);
                    continue;
                }
                else if (c == ']' && inCharSet)
                {
                    inCharSet = false;
                    regexPattern.Append(c);
                    continue;
                }

                if (inCharSet)
                {
                    // Directly add character set contents to regex pattern
                    regexPattern.Append(c);
                }
                else
                {
                    switch (c)
                    {
                        case '%':
                            regexPattern.Append(escapeNext ? "%" : ".*");
                            break;
                        case '_':
                            regexPattern.Append(escapeNext ? "_" : ".");
                            break;
                        default:
                            if ("+()^$.{}[]|\\".Contains(c))
                            {
                                regexPattern.Append("\\" + c); // Escape regex special characters
                            }
                            else
                            {
                                regexPattern.Append(c);
                            }
                            break;
                    }
                }

                if (escapeNext) escapeNext = false; // Reset escape flag if it was set
            }

            // Add start and end anchors to ensure the entire string is matched
            regexPattern.Append('$'); // Add end anchor
            return regexPattern.ToString();
        }

        private static FlxValue StringBase64EncodeImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(Convert.ToBase64String(val.AsFlxString.Span)));
        }

        private static FlxValue StringBase64DecodeImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            var bytes = Convert.FromBase64String(val.AsString);
            var str = Encoding.UTF8.GetString(bytes);
            return FlxValue.FromBytes(FlexBuffer.SingleValue(str));
        }

        private static FlxValue CharLengthImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }
            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.Length));
        }
    }
}
