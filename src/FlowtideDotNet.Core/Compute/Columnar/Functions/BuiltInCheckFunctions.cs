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
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Encoders;
using FlowtideDotNet.Core.Flexbuffer;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInCheckFunctions
    {
        public static void RegisterCheckFunctions(IFunctionsRegister functionsRegister)
        {
            /*
             * Implements
             * 
             * if (condition.Type == ArrowTypeId.Boolean && !condition.AsBool && message.Type == ArrowTypeId.String)
             * {
             *   If any tags they are computed here
             *   tags[0] = new KeyValuePair<string, object>(tagName, deserialize(tagValue1));
             *   notificationReciever.Notify(message, tags);
             * }
             * return value;
             */
            functionsRegister.RegisterColumnScalarFunction(FunctionsCheck.Uri, FunctionsCheck.CheckValue,
                (func, parameterInfo, visitor, functionServices) =>
                {
                    if (func.Arguments.Count < 3)
                    {
                        throw new InvalidOperationException("Check function requires at least 3 arguments.");
                    }
                    
                    if ((func.Arguments.Count - 3) % 2 != 0)
                    {
                        throw new InvalidOperationException("Check function has invalid tag arguments, they must come in pairs with key and then value.");
                    }

                    var numberofTags = (func.Arguments.Count - 3) / 2;

                    var checkValue = visitor.Visit(func.Arguments[0], parameterInfo);
                    var checkCondition = visitor.Visit(func.Arguments[1], parameterInfo);
                    var checkMessage = visitor.Visit(func.Arguments[2], parameterInfo);

                    if (checkValue == null || checkCondition == null || checkMessage == null)
                    {
                        throw new InvalidOperationException("Check function arguments could not be compiled.");
                    }

                    var checkConditionTemporaryVariable = Expression.Variable(checkCondition.Type, "checkCondition");
                    var checkConditionAssign = Expression.Assign(checkConditionTemporaryVariable, checkCondition);

                    var checkMessageTemporaryVariable = Expression.Variable(checkMessage.Type, "checkMessage");
                    var checkMessageAssign = Expression.Assign(checkMessageTemporaryVariable, checkMessage);

                    var notificationReceiverConstant = Expression.Constant(functionServices.CheckNotificationReceiver);
                    var conditionTypeProp = Expression.Property(checkConditionTemporaryVariable, nameof(IDataValue.Type));
                    var conditionAsBoolProp = Expression.Property(checkConditionTemporaryVariable, nameof(IDataValue.AsBool));
                    var messageTypeProp = Expression.Property(checkMessageTemporaryVariable, nameof(IDataValue.Type));

                    // Create the deserialization method to create a C# object
                    var unionResolver = new ObjectConverterResolver();
                    var objectConverter = unionResolver.GetConverter(ObjectConverterTypeInfoLookup.GetTypeInfo(typeof(object)));
                    var objectConverterConstant = Expression.Constant(objectConverter);
                    var deserializeMethod = typeof(IObjectColumnConverter).GetMethod(nameof(IObjectColumnConverter.Deserialize), BindingFlags.Public | BindingFlags.Instance);

                    if (deserializeMethod == null)
                    {
                        throw new InvalidOperationException("Deserialize method not found");
                    }

                    // Build creation of the span here
                    var createSpanCtor = typeof(ReadOnlySpan<KeyValuePair<string, object?>>).GetConstructor([typeof(KeyValuePair<string, object?>[])]);

                    if (createSpanCtor == null)
                    {
                        throw new InvalidOperationException("Constructor for ReadOnlySpan<KeyValuePair<string, object>> not found");
                    }

                    List<Expression> blockExpressions = new List<Expression>();
                    var keyValuePairCreateMethod = typeof(KeyValuePair).GetMethod("Create", BindingFlags.Static | BindingFlags.Public);
                    if (keyValuePairCreateMethod == null)
                    {
                        throw new InvalidOperationException("KeyValuePair.Create method not found");
                    }

                    var genericCreateKeyValuePair = keyValuePairCreateMethod.MakeGenericMethod(typeof(string), typeof(object));
                    var staticArray = Expression.Constant(new KeyValuePair<string, object?>[numberofTags]);
                    for (int i = 3; i < func.Arguments.Count; i += 2)
                    {
                        if (func.Arguments[i] is StringLiteral stringLiteral)
                        {
                            var propName = Expression.Constant(stringLiteral.Value);
                            var tagValueExpr = visitor.Visit(func.Arguments[i + 1], parameterInfo);

                            if (tagValueExpr == null)
                            {
                                throw new InvalidOperationException("Check function arguments could not be compiled.");
                            }

                            var deserializeGenericMethod = deserializeMethod.MakeGenericMethod(tagValueExpr.Type);

                            var tagValue = Expression.Call(objectConverterConstant, deserializeGenericMethod, tagValueExpr);
                            var createKeyValuePair = Expression.Call(genericCreateKeyValuePair, propName, tagValue);
                            var arrayIndex = Expression.ArrayAccess(staticArray, Expression.Constant((i - 3) / 2));
                            var elementAssign = Expression.Assign(arrayIndex, createKeyValuePair);
                            blockExpressions.Add(elementAssign);
                        }
                        else
                        {
                            throw new InvalidOperationException("Check function arguments could not be compiled.");
                        }
                    }
                    var createSpan = Expression.New(createSpanCtor, staticArray);

                    var notificationReceiverMethod = typeof(ICheckNotificationReceiver).GetMethod(nameof(ICheckNotificationReceiver.OnCheckFailure), BindingFlags.Public | BindingFlags.Instance);
                    var flxStringToStringMethod = typeof(FlxString).GetMethod(nameof(FlxString.ToString), BindingFlags.Public | BindingFlags.Instance);
                    var messageAsStringProp = checkMessage.Type.GetProperty(nameof(IDataValue.AsString));

                    if (notificationReceiverMethod == null)
                    {
                        throw new InvalidOperationException("OnCheckFailure method not found");
                    }
                    if (flxStringToStringMethod == null)
                    {
                        throw new InvalidOperationException("FlxString.ToString method not found");
                    }
                    if (messageAsStringProp == null)
                    {
                        throw new InvalidOperationException("AsString property not found");
                    }

                    var messageAsString = Expression.Property(checkMessageTemporaryVariable, messageAsStringProp);
                    var messageAsStringCall = Expression.Call(messageAsString, flxStringToStringMethod);

                    if (messageAsString == null)
                    {
                        throw new InvalidOperationException("Message as string property not found");
                    }

                    var callNotification = Expression.Call(notificationReceiverConstant, notificationReceiverMethod, messageAsStringCall, createSpan);
                    blockExpressions.Add(callNotification);

                    var block = Expression.Block(blockExpressions);

                    var checckIfMessageString = Expression.IfThen(Expression.Equal(messageTypeProp, Expression.Constant(ArrowTypeId.String)), block);
                    var checkMessageBlock = Expression.Block(new[] { checkMessageTemporaryVariable }, checkMessageAssign, checckIfMessageString);
                    var checkIfContiditionNotTrue = Expression.IfThen(Expression.Not(conditionAsBoolProp), checkMessageBlock);
                    var checkIfConditionIsBoolean = Expression.IfThen(Expression.Equal(conditionTypeProp, Expression.Constant(ArrowTypeId.Boolean)), checkIfContiditionNotTrue);

                    var returnBlockExpr = Expression.Block(typeof(IDataValue), new[] { checkConditionTemporaryVariable }, [checkConditionAssign, checkIfConditionIsBoolean, checkValue]);

                    return returnBlockExpr;
                });

            functionsRegister.RegisterColumnScalarFunction(FunctionsCheck.Uri, FunctionsCheck.CheckTrue,
                (func, parameterInfo, visitor, functionServices) =>
                {
                    if (func.Arguments.Count < 2)
                    {
                        throw new InvalidOperationException("check_true function requires at least 2 arguments.");
                    }

                    if ((func.Arguments.Count - 2) % 2 != 0)
                    {
                        throw new InvalidOperationException("check_true function has invalid tag arguments, they must come in pairs with key and then value.");
                    }

                    var numberofTags = (func.Arguments.Count - 2) / 2;

                    var checkCondition = visitor.Visit(func.Arguments[0], parameterInfo);
                    var checkMessage = visitor.Visit(func.Arguments[1], parameterInfo);

                    if (checkCondition == null || checkMessage == null)
                    {
                        throw new InvalidOperationException("check_true function arguments could not be compiled.");
                    }

                    var checkConditionTemporaryVariable = Expression.Variable(checkCondition.Type, "checkCondition");
                    var checkConditionAssign = Expression.Assign(checkConditionTemporaryVariable, checkCondition);

                    var checkMessageTemporaryVariable = Expression.Variable(checkMessage.Type, "checkMessage");
                    var checkMessageAssign = Expression.Assign(checkMessageTemporaryVariable, checkMessage);

                    var notificationReceiverConstant = Expression.Constant(functionServices.CheckNotificationReceiver);
                    var conditionTypeProp = Expression.Property(checkConditionTemporaryVariable, nameof(IDataValue.Type));
                    var conditionAsBoolProp = Expression.Property(checkConditionTemporaryVariable, nameof(IDataValue.AsBool));
                    var messageTypeProp = Expression.Property(checkMessageTemporaryVariable, nameof(IDataValue.Type));

                    // Create the deserialization method to create a C# object
                    var unionResolver = new ObjectConverterResolver();
                    var objectConverter = unionResolver.GetConverter(ObjectConverterTypeInfoLookup.GetTypeInfo(typeof(object)));
                    var objectConverterConstant = Expression.Constant(objectConverter);
                    var deserializeMethod = typeof(IObjectColumnConverter).GetMethod(nameof(IObjectColumnConverter.Deserialize), BindingFlags.Public | BindingFlags.Instance);

                    if (deserializeMethod == null)
                    {
                        throw new InvalidOperationException("Deserialize method not found");
                    }

                    // Build creation of the span here
                    var createSpanCtor = typeof(ReadOnlySpan<KeyValuePair<string, object?>>).GetConstructor([typeof(KeyValuePair<string, object?>[])]);

                    if (createSpanCtor == null)
                    {
                        throw new InvalidOperationException("Constructor for ReadOnlySpan<KeyValuePair<string, object>> not found");
                    }

                    List<Expression> blockExpressions = new List<Expression>();
                    var keyValuePairCreateMethod = typeof(KeyValuePair).GetMethod("Create", BindingFlags.Static | BindingFlags.Public);
                    if (keyValuePairCreateMethod == null)
                    {
                        throw new InvalidOperationException("KeyValuePair.Create method not found");
                    }

                    var genericCreateKeyValuePair = keyValuePairCreateMethod.MakeGenericMethod(typeof(string), typeof(object));
                    var staticArray = Expression.Constant(new KeyValuePair<string, object?>[numberofTags]);
                    for (int i = 2; i < func.Arguments.Count; i += 2)
                    {
                        if (func.Arguments[i] is StringLiteral stringLiteral)
                        {
                            var propName = Expression.Constant(stringLiteral.Value);
                            var tagValueExpr = visitor.Visit(func.Arguments[i + 1], parameterInfo);

                            if (tagValueExpr == null)
                            {
                                throw new InvalidOperationException("Check function arguments could not be compiled.");
                            }

                            var deserializeGenericMethod = deserializeMethod.MakeGenericMethod(tagValueExpr.Type);

                            var tagValue = Expression.Call(objectConverterConstant, deserializeGenericMethod, tagValueExpr);
                            var createKeyValuePair = Expression.Call(genericCreateKeyValuePair, propName, tagValue);
                            var arrayIndex = Expression.ArrayAccess(staticArray, Expression.Constant((i - 3) / 2));
                            var elementAssign = Expression.Assign(arrayIndex, createKeyValuePair);
                            blockExpressions.Add(elementAssign);
                        }
                        else
                        {
                            throw new InvalidOperationException("Check function arguments could not be compiled.");
                        }
                    }
                    var createSpan = Expression.New(createSpanCtor, staticArray);

                    var notificationReceiverMethod = typeof(ICheckNotificationReceiver).GetMethod(nameof(ICheckNotificationReceiver.OnCheckFailure), BindingFlags.Public | BindingFlags.Instance);
                    var flxStringToStringMethod = typeof(FlxString).GetMethod(nameof(FlxString.ToString), BindingFlags.Public | BindingFlags.Instance);
                    var messageAsStringProp = checkMessage.Type.GetProperty(nameof(IDataValue.AsString));

                    if (notificationReceiverMethod == null)
                    {
                        throw new InvalidOperationException("OnCheckFailure method not found");
                    }
                    if (flxStringToStringMethod == null)
                    {
                        throw new InvalidOperationException("FlxString.ToString method not found");
                    }
                    if (messageAsStringProp == null)
                    {
                        throw new InvalidOperationException("AsString property not found");
                    }

                    var messageAsString = Expression.Property(checkMessageTemporaryVariable, messageAsStringProp);
                    var messageAsStringCall = Expression.Call(messageAsString, flxStringToStringMethod);

                    if (messageAsString == null)
                    {
                        throw new InvalidOperationException("Message as string property not found");
                    }

                    var callNotification = Expression.Call(notificationReceiverConstant, notificationReceiverMethod, messageAsStringCall, createSpan);
                    blockExpressions.Add(callNotification);

                    // Use a container as return value to skip heap allocations casting BoolValue to IDataValue
                    var returnDataValueContainer = new DataValueContainer();
                    returnDataValueContainer._type = ArrowTypeId.Boolean;
                    var returnConstant = Expression.Constant(returnDataValueContainer);

                    var returnDataBoolValueField = Expression.Field(returnConstant, nameof(DataValueContainer._boolValue));
                    var assignReturnDataToFalse = Expression.Assign(returnDataBoolValueField, Expression.Constant(BoolValue.False));
                    var assignReturnDataToTrue = Expression.Assign(returnDataBoolValueField, Expression.Constant(BoolValue.True));

                    blockExpressions.Add(assignReturnDataToFalse);

                    var block = Expression.Block(blockExpressions);

                    var checckIfMessageString = Expression.IfThenElse(Expression.Equal(messageTypeProp, Expression.Constant(ArrowTypeId.String)), block, assignReturnDataToFalse);

                    // Create a block to assign the computed message to the temporary variable
                    var checkMessageBlock = Expression.Block(new[] { checkMessageTemporaryVariable }, checkMessageAssign, checckIfMessageString);
                    var checkIfContiditionNotTrue = Expression.IfThenElse(Expression.Not(conditionAsBoolProp), checkMessageBlock, assignReturnDataToTrue);
                    var checkIfConditionIsBoolean = Expression.IfThenElse(Expression.Equal(conditionTypeProp, Expression.Constant(ArrowTypeId.Boolean)), checkIfContiditionNotTrue, assignReturnDataToFalse);

                    var returnBlockExpr = Expression.Block(typeof(IDataValue), new[] { checkConditionTemporaryVariable }, [checkConditionAssign, checkIfConditionIsBoolean, returnConstant]);

                    return returnBlockExpr;
                });
        }
    }
}
