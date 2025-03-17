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
using FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.StringAgg;
using FlowtideDotNet.Core.Flexbuffer;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInStringFunctions
    {
        private const string NegativeStart = "negative_start";
        private const string WrapFromEnd = "WRAP_FROM_END";
        private const string LeftOfBeginning = "LEFT_OF_BEGINNING";
        private const string NullHandling = "null_handling";
        private const string AcceptNulls = "ACCEPT_NULLS";
        private const string IgnoreNulls = "IGNORE_NULLS";

        private static readonly StringValue EmptyString = new StringValue("");
        private static readonly StringValue BackslashString = new StringValue("\\");

        public static void RegisterFunctions(IFunctionsRegister functionsRegister)
        {
            ColumnStringAggAggregation.Register(functionsRegister);

            functionsRegister.RegisterColumnScalarFunction(FunctionsString.Uri, FunctionsString.Substring,
                (scalarFunction, parameters, visitor) =>
                {
                    if (scalarFunction.Arguments.Count < 2)
                    {
                        throw new InvalidOperationException("Substring function must have atleast 2 arguments");
                    }

                    var expr = visitor.Visit(scalarFunction.Arguments[0], parameters)!;
                    var start = visitor.Visit(scalarFunction.Arguments[1], parameters)!;

                    Expression? length = default;
                    if (scalarFunction.Arguments.Count == 3)
                    {
                        length = visitor.Visit(scalarFunction.Arguments[2], parameters)!;
                    }
                    else
                    {
                        length = Expression.Constant(new Int64Value(-1));
                    }

                    string methodName = nameof(Substring);
                    if (scalarFunction.Options != null && scalarFunction.Options.TryGetValue(NegativeStart, out var negativeStart))
                    {
                        if (negativeStart == WrapFromEnd)
                        {
                            methodName = nameof(SubstringWrapFromEnd);
                        }
                        else if (negativeStart == LeftOfBeginning)
                        {
                            methodName = nameof(SubstringLeftOfBeginning);
                        }
                        else
                        {
                            throw new NotSupportedException($"Substring option {NegativeStart}={negativeStart} is not supported.");
                        }
                    }


                    MethodInfo? toStringMethod = typeof(BuiltInStringFunctions).GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
                    Debug.Assert(toStringMethod != null);
                    var genericMethod = toStringMethod.MakeGenericMethod(expr.Type, start.Type, length.Type);
                    var resultContainer = Expression.Constant(new DataValueContainer());
                    return System.Linq.Expressions.Expression.Call(genericMethod, expr, start, length, resultContainer);
                });

            functionsRegister.RegisterColumnScalarFunction(FunctionsString.Uri, FunctionsString.Concat,
                (func, parameters, visitor) =>
                {
                    bool ignoreNulls = false;

                    if (func.Options != null && func.Options.TryGetValue(NullHandling, out var nullHandling) && nullHandling == IgnoreNulls)
                    {
                        ignoreNulls = true;
                    }

                    List<System.Linq.Expressions.Expression> expressions = new List<System.Linq.Expressions.Expression>();
                    var stringBuilder = new StringBuilder();

                    var appendMethod = typeof(StringBuilder).GetMethod("Append", new System.Type[] { typeof(string) });
                    var toStringMethod = typeof(FlxString).GetMethod("ToString", new System.Type[] { });
                    Debug.Assert(appendMethod != null);
                    Debug.Assert(toStringMethod != null);

                    var stringBuilderConstant = System.Linq.Expressions.Expression.Constant(stringBuilder);

                    DataValueContainer nullContainer = new DataValueContainer();
                    nullContainer._type = ArrowTypeId.Null;
                    var nullConstant = System.Linq.Expressions.Expression.Constant(nullContainer);

                    DataValueContainer temporaryContainer = new DataValueContainer();
                    var temporaryVariable = System.Linq.Expressions.Expression.Constant(temporaryContainer);

                    var returnTarget = System.Linq.Expressions.Expression.Label(typeof(IDataValue));

                    var stringBuilderToStringExpr = System.Linq.Expressions.Expression.Call(stringBuilderConstant, "ToString", new System.Type[] { });


                    var newStringValueExpr = System.Linq.Expressions.Expression.New(typeof(StringValue).GetConstructor(new System.Type[] { typeof(string) })!, stringBuilderToStringExpr);
                    var castToIDataValue = System.Linq.Expressions.Expression.Convert(newStringValueExpr, typeof(IDataValue));
                    var returnLabel = System.Linq.Expressions.Expression.Label(returnTarget, castToIDataValue);

                    var nullValueReturn = System.Linq.Expressions.Expression.Return(returnTarget, nullConstant);

                    List<System.Linq.Expressions.Expression> blockExpressions = new List<System.Linq.Expressions.Expression>();

                    var stringBuilderClearExpr = System.Linq.Expressions.Expression.Call(stringBuilderConstant, "Clear", new System.Type[] { });

                    // Start with clearing the string builder
                    blockExpressions.Add(stringBuilderClearExpr);

                    foreach (var expr in func.Arguments)
                    {
                        var v = visitor.Visit(expr, parameters)!;
                        var typeField = System.Linq.Expressions.Expression.PropertyOrField(v, "Type");
                        var typeIsNullCheck = System.Linq.Expressions.Expression.Equal(typeField, System.Linq.Expressions.Expression.Constant(ArrowTypeId.Null));
                        var typeIsStringCheck = System.Linq.Expressions.Expression.Equal(typeField, System.Linq.Expressions.Expression.Constant(ArrowTypeId.String));

                        var asStringField = System.Linq.Expressions.Expression.PropertyOrField(v, "AsString");
                        var toStringCall = System.Linq.Expressions.Expression.Call(asStringField, toStringMethod);
                        var castToString = ColumnCastImplementations.CallCastToString(v, temporaryVariable);
                        var asStringFromCastToString = System.Linq.Expressions.Expression.PropertyOrField(castToString, "AsString");
                        var toStringFromCastToString = System.Linq.Expressions.Expression.Call(asStringFromCastToString, toStringMethod);
                        var appendLine = System.Linq.Expressions.Expression.Call(stringBuilderConstant, appendMethod, toStringCall);
                        var appendLineCastedToString = System.Linq.Expressions.Expression.Call(stringBuilderConstant, appendMethod, toStringFromCastToString);
                        var checkIfString = System.Linq.Expressions.Expression.IfThenElse(typeIsStringCheck, appendLine, appendLineCastedToString);
                        System.Linq.Expressions.Expression? nullCheck = default;
                        if (ignoreNulls)
                        {
                            nullCheck = System.Linq.Expressions.Expression.IfThen(typeIsStringCheck, checkIfString);
                        }
                        else
                        {
                            nullCheck = System.Linq.Expressions.Expression.IfThenElse(typeIsNullCheck, nullValueReturn, checkIfString);
                        }
                        blockExpressions.Add(nullCheck);
                    }
                    blockExpressions.Add(returnLabel);
                    var blockExpression = System.Linq.Expressions.Expression.Block(blockExpressions);


                    return blockExpression;
                });

            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.Upper, typeof(BuiltInStringFunctions), nameof(UpperImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.Lower, typeof(BuiltInStringFunctions), nameof(LowerImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.Trim, typeof(BuiltInStringFunctions), nameof(TrimImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.LTrim, typeof(BuiltInStringFunctions), nameof(LTrimImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.RTrim, typeof(BuiltInStringFunctions), nameof(RTrimImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.To_String, typeof(BuiltInStringFunctions), nameof(ToStringImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.StartsWith, typeof(BuiltInStringFunctions), nameof(StartsWithImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.Like, typeof(BuiltInStringFunctions), nameof(LikeImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.Replace, typeof(BuiltInStringFunctions), nameof(ReplaceImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.StringBase64Encode, typeof(BuiltInStringFunctions), nameof(StringBase64EncodeImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.StringBase64Decode, typeof(BuiltInStringFunctions), nameof(StringBase64DecodeImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.CharLength, typeof(BuiltInStringFunctions), nameof(CharLengthImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsString.Uri, FunctionsString.StrPos, typeof(BuiltInStringFunctions), nameof(StrPosImplementation));
        }

        private static bool SubstringTryGetParameters<T1, T2, T3>(
            T1 value,
            T2 start,
            T3 length,
            [NotNullWhen(true)] out string? stringVal,
            out int startInt,
            out int lengthInt)
            where T1 : IDataValue
            where T2 : IDataValue
            where T3 : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                stringVal = default;
                startInt = 0;
                lengthInt = 0;
                return false;
            }
            if (start.Type != ArrowTypeId.Int64)
            {
                stringVal = default;
                startInt = 0;
                lengthInt = 0;
                return false;
            }
            if (length.Type != ArrowTypeId.Int64)
            {
                stringVal = default;
                startInt = 0;
                lengthInt = 0;
                return false;
            }

            stringVal = value.AsString.ToString();
            startInt = (int)start.AsLong;
            lengthInt = (int)length.AsLong;
            return true;
        }

        private static IDataValue SubstringLeftOfBeginning<T1, T2, T3>(T1 value, T2 start, T3 length, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
            where T3 : IDataValue
        {
            if (!SubstringTryGetParameters(value, start, length, out var str, out var startInt, out var lengthInt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            if (startInt > str.Length)
            {
                result._type = ArrowTypeId.String;
                result._stringValue = EmptyString;
                return result;
            }

            if (startInt < 0)
            {
                lengthInt = lengthInt + startInt - 1; // Negate by -1 to cover 0 
                startInt = 1;
            }

            if (lengthInt < -1)
            {
                lengthInt = str.Length - startInt + 1;
            }
            else
            {
                lengthInt = Math.Min(lengthInt, str.Length - startInt + 1);
            }
            result._type = ArrowTypeId.String;
            var stringInfo = new StringInfo(str);
            result._stringValue = new StringValue(stringInfo.SubstringByTextElements(startInt - 1, lengthInt));
            return result;
        }

        private static IDataValue SubstringWrapFromEnd<T1, T2, T3>(T1 value, T2 start, T3 length, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
            where T3 : IDataValue
        {
            if (!SubstringTryGetParameters(value, start, length, out var str, out var startInt, out var lengthInt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            if (startInt > str.Length)
            {
                result._type = ArrowTypeId.String;
                result._stringValue = EmptyString;
                return result;
            }

            if (startInt < 0)
            {
                startInt = str.Length + startInt + 1;
            }

            if (lengthInt == -1)
            {
                lengthInt = str.Length - startInt + 1;
            }
            else
            {
                lengthInt = Math.Min(lengthInt, str.Length - startInt + 1);
            }
            result._type = ArrowTypeId.String;
            var stringInfo = new StringInfo(str);
            result._stringValue = new StringValue(stringInfo.SubstringByTextElements(startInt - 1, lengthInt));
            return result;
        }

        private static IDataValue Substring<T1, T2, T3>(T1 value, T2 start, T3 length, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
            where T3 : IDataValue
        {
            if (!SubstringTryGetParameters(value, start, length, out var str, out var startInt, out var lengthInt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            if (startInt > str.Length)
            {
                result._type = ArrowTypeId.String;
                result._stringValue = EmptyString;
                return result;
            }
            if (lengthInt == -1)
            {
                lengthInt = str.Length - startInt + 1;
            }
            else
            {
                lengthInt = Math.Min(lengthInt, str.Length - startInt + 1);
            }
            result._type = ArrowTypeId.String;
            var stringInfo = new StringInfo(str);
            result._stringValue = new StringValue(stringInfo.SubstringByTextElements(startInt - 1, lengthInt));
            return result;
        }

        private static IDataValue UpperImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.String;
            result._stringValue = new StringValue(value.AsString.ToString().ToUpper());
            return result;
        }

        private static IDataValue LowerImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.String;
            result._stringValue = new StringValue(value.AsString.ToString().ToLower());
            return result;
        }

        private static IDataValue TrimImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.String;
            result._stringValue = new StringValue(value.AsString.ToString().Trim());
            return result;
        }

        /// <summary>
        /// Trim with a string with to trim info this method cant use built in C# trim since it allows strings and not just chars
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <param name="value"></param>
        /// <param name="toTrim"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        private static IDataValue TrimImplementation<T1, T2>(T1 value, T2 toTrim, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            if (toTrim.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.String;
            var toTrimStr = toTrim.AsString.ToString();
            var valueStr = value.AsString.ToString();

            result._stringValue = new StringValue(valueStr.Trim(toTrimStr.ToCharArray()));
            return result;
        }

        private static IDataValue LTrimImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.String;
            result._stringValue = new StringValue(value.AsString.ToString().TrimStart());
            return result;
        }

        private static IDataValue LTrimImplementation<T1, T2>(T1 value, T2 toTrim, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            if (toTrim.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.String;
            var toTrimStr = toTrim.AsString.ToString();
            var valueStr = value.AsString.ToString();

            result._type = ArrowTypeId.String;
            result._stringValue = new StringValue(valueStr.TrimStart(toTrimStr.ToCharArray()));
            return result;
        }

        private static IDataValue RTrimImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.String;
            result._stringValue = new StringValue(value.AsString.ToString().TrimEnd());
            return result;
        }

        private static IDataValue RTrimImplementation<T1, T2>(T1 value, T2 toTrim, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            if (toTrim.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.String;
            var toTrimStr = toTrim.AsString.ToString();
            var valueStr = value.AsString.ToString();

            result._type = ArrowTypeId.String;
            result._stringValue = new StringValue(valueStr.TrimEnd(toTrimStr.ToCharArray()));
            return result;
        }

        private static IDataValue ToStringImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            return ColumnCastImplementations.CastToString(value, result, new ColumnCastImplementations.CastToStringContainer());
        }

        private static IDataValue StartsWithImplementation<T1, T2>(T1 value, T2 prefix, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (value.Type != ArrowTypeId.String || prefix.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(false);
                return result;
            }

            result._type = ArrowTypeId.Boolean;
            result._boolValue = new BoolValue(value.AsString.ToString().StartsWith(prefix.AsString.ToString()));
            return result;
        }

        private static IDataValue StartsWithImplementation_case_sensitivity__CASE_INSENSITIVE<T1, T2>(T1 value, T2 prefix, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (value.Type != ArrowTypeId.String || prefix.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(false);
                return result;
            }

            result._type = ArrowTypeId.Boolean;
            result._boolValue = new BoolValue(value.AsString.ToString().StartsWith(prefix.AsString.ToString(), StringComparison.InvariantCultureIgnoreCase));
            return result;
        }

        private static IDataValue LikeImplementation<T1, T2>(T1 value, T2 comp, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            return LikeImplementation(value, comp, BackslashString, result);
        }

        private static IDataValue LikeImplementation<T1, T2, T3>(T1 value, T2 comp, T3 escapeChar, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
            where T3 : IDataValue
        {
            if (value.Type != ArrowTypeId.String || comp.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(false);
                return result;
            }

            char? escapeCharacter = default;
            // Check if escape char is set
            if (escapeChar.Type == ArrowTypeId.String)
            {
                escapeCharacter = escapeChar.AsString.ToString()[0];
            }

            var likeResult = IsLike(value.AsString.ToString(), comp.AsString.ToString(), escapeCharacter);

            result._type = ArrowTypeId.Boolean;
            result._boolValue = new BoolValue(likeResult);
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

        private static IDataValue ReplaceImplementation<T1, T2, T3>(T1 value, T2 pattern, T3 replacement, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
            where T3 : IDataValue
        {
            if (value.Type != ArrowTypeId.String || pattern.Type != ArrowTypeId.String || replacement.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.String;
            result._stringValue = new StringValue(value.AsString.ToString().Replace(pattern.AsString.ToString(), replacement.AsString.ToString()));
            return result;
        }

        private static IDataValue StringBase64EncodeImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.String;
            result._stringValue = new StringValue(Convert.ToBase64String(Encoding.UTF8.GetBytes(value.AsString.ToString())));
            return result;
        }

        private static IDataValue StringBase64DecodeImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            try
            {
                result._type = ArrowTypeId.String;
                result._stringValue = new StringValue(Encoding.UTF8.GetString(Convert.FromBase64String(value.AsString.ToString())));
            }
            catch (FormatException)
            {
                result._type = ArrowTypeId.Null;
            }

            return result;
        }

        private static IDataValue CharLengthImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.Int64;

            result._int64Value = new Int64Value(new StringInfo(value.AsString.ToString()).LengthInTextElements);
            return result;
        }

        private static IDataValue StrPosImplementation<T1, T2>(T1 value, T2 toFind, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (value.Type != ArrowTypeId.String || toFind.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(value.AsString.ToString().IndexOf(toFind.AsString.ToString()) + 1);
            return result;
        }

        private static IDataValue StrPosImplementation_case_sensitivity__CASE_INSENSITIVE<T1, T2>(T1 value, T2 toFind, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (value.Type != ArrowTypeId.String || toFind.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(value.AsString.ToString().IndexOf(toFind.AsString.ToString(), StringComparison.InvariantCultureIgnoreCase) + 1);
            return result;
        }

    }
}
