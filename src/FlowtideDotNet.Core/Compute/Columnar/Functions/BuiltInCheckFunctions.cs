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

using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInCheckFunctions
    {
        public static void RegisterCheckFunctions(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterColumnScalarFunction(FunctionsCheck.Uri, FunctionsCheck.CheckValue,
                (func, parameterInfo, visitor, functionServices) =>
                {
                    var checkValue = visitor.Visit(func.Arguments[0], parameterInfo);
                    var checkCondition = visitor.Visit(func.Arguments[1], parameterInfo);
                    var checkMessage = visitor.Visit(func.Arguments[2], parameterInfo);

                    if (checkValue == null || checkCondition == null || checkMessage == null)
                    {
                        throw new InvalidOperationException("Check function arguments could not be compiled.");
                    }

                    var method = typeof(BuiltInCheckFunctions).GetMethod(nameof(CheckValueImplementation), BindingFlags.NonPublic | BindingFlags.Static);

                    if (method == null)
                    {
                        throw new InvalidOperationException("Check implementation method not found");
                    }

                    var genericMethod = method.MakeGenericMethod(
                        checkValue.Type,
                        checkCondition.Type,
                        checkMessage.Type
                    );

                    return System.Linq.Expressions.Expression.Call(
                        genericMethod,
                        checkValue,
                        checkCondition,
                        checkMessage,
                        System.Linq.Expressions.Expression.Constant(functionServices.CheckNotificationReceiver)
                    );
                });

            functionsRegister.RegisterColumnScalarFunction(FunctionsCheck.Uri, FunctionsCheck.CheckTrue,
                (func, parameterInfo, visitor, functionServices) =>
                {
                    var checkCondition = visitor.Visit(func.Arguments[0], parameterInfo);
                    var checkMessage = visitor.Visit(func.Arguments[1], parameterInfo);

                    if (checkCondition == null || checkMessage == null)
                    {
                        throw new InvalidOperationException("Check function arguments could not be compiled.");
                    }

                    var method = typeof(BuiltInCheckFunctions).GetMethod(nameof(CheckTrueImplementation), BindingFlags.NonPublic | BindingFlags.Static);

                    if (method == null)
                    {
                        throw new InvalidOperationException("Check implementation method not found");
                    }

                    var genericMethod = method.MakeGenericMethod(
                        checkCondition.Type,
                        checkMessage.Type
                    );

                    return System.Linq.Expressions.Expression.Call(
                        genericMethod,
                        checkCondition,
                        checkMessage,
                        System.Linq.Expressions.Expression.Constant(functionServices.CheckNotificationReceiver)
                    );
                });
        }

        private static IDataValue CheckValueImplementation<T1, T2, T3>(T1 value, T2 condition, T3 message, ICheckNotificatioReceiver notificationReceiver)
            where T1 : IDataValue
            where T2 : IDataValue
            where T3 : IDataValue
        {
            if (condition.Type == ArrowTypeId.Boolean && !condition.AsBool && message.Type == ArrowTypeId.String)
            {
                notificationReceiver.OnCheckFailure(message.AsString.Span);
            }
            return value;
        }

        private static IDataValue CheckTrueImplementation<T1, T2>(T1 condition, T2 message, ICheckNotificatioReceiver notificationReceiver)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (condition.Type == ArrowTypeId.Boolean)
            {
                if (!condition.AsBool && message.Type == ArrowTypeId.String)
                {
                    notificationReciever.OnCheckFailure(message.AsString.Span);
                }
                return condition;
            }
            return BoolValue.False;
        }
    }
}
