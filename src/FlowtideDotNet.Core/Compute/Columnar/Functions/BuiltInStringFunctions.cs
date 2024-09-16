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
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInStringFunctions
    {
        public static void RegisterFunctions(IFunctionsRegister functionsRegister)
        {
            ColumnStringAggAggregation.Register(functionsRegister);
            
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

        private static IDataValue ToStringImplementation<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            return ColumnCastImplementations.CastToString(value, result);
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
            result._int64Value = new Int64Value(value.AsString.ToString().Length);
            return result;
        }

    }
}
