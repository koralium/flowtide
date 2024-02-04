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

using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using OpenFga.Sdk.Model;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    internal partial class FlowtideOpenFgaModelParser
    {
        private static FilterRelation CreateRelationTypeFilter(Relation input, string relation, string type, HashSet<ResultUserType>? userTypes = null)
        {
            
            var andArguments = new List<Expression>()
            {
                new ScalarFunction()
                {
                    ExtensionUri = FunctionsComparison.Uri,
                    ExtensionName = FunctionsComparison.Equal,
                    Arguments = new List<Expression>()
                    {
                        new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment()
                            {
                                Field = RelationColumn // Field 1 is the relation field
                            }
                        },
                        new StringLiteral()
                        {
                            Value = relation
                        }
                    }
                },
                new ScalarFunction()
                {
                    ExtensionUri = FunctionsComparison.Uri,
                    ExtensionName = FunctionsComparison.Equal,
                    Arguments = new List<Expression>()
                    {
                        new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment()
                            {
                                Field = ObjectTypeColumn
                            }
                        },
                        new StringLiteral()
                        {
                            Value = type
                        }
                    }
                }
            };

            if (userTypes != null)
            {
                var orList = new SingularOrListExpression()
                {
                    Value = new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment()
                        {
                            Field = UserTypeColumn
                        }
                    },
                    Options = userTypes.Select(x => (Expression)new StringLiteral() { Value = x.TypeName }).ToList()
                };
                andArguments.Add(orList);
            }
            return new FilterRelation()
            {
                Input = input,
                Condition = new ScalarFunction()
                {
                    ExtensionName = FunctionsBoolean.And,
                    ExtensionUri = FunctionsBoolean.Uri,
                    Arguments = andArguments
                }
            };
        }

        private static FilterRelation CreateUserRelationFilter(
            Relation input, 
            string userType, 
            string userRelation, 
            string objectRelation,
            string objectType)
        {
            return new FilterRelation()
            {
                Input = input,
                // user_relation = {relationReference.Relation}' AND relation = {toRelationName} AND object_type = {objectType.Type} AND user_type = {relationReference.Type}
                Condition = new ScalarFunction()
                {
                    ExtensionName = FunctionsBoolean.And,
                    ExtensionUri = FunctionsBoolean.Uri,
                    Arguments = new List<Expression>()
                    {
                        // user_type = {relationReference.Type}
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = UserTypeColumn
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = userType
                                }
                            }
                        },
                        // user_relation = {relationReference.Relation}'
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = UserRelationColumn
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = userRelation
                                }
                            }
                        },
                        // relation = {toRelationName}
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = RelationColumn
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = objectRelation
                                }
                            }
                        },
                        // object_type = {objectType.Type}
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = ObjectTypeColumn // Field 3 is the object type field
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = objectType
                                }
                            }
                        }
                    }
                }
            };
        }

        private static JoinRelation CreateJoinRelationUserTypeAndId(Relation left, Relation right)
        {
            return new JoinRelation()
            {
                Emit = new List<int>() { UserTypeColumn, UserIdColumn, UserRelationColumn, RelationColumn, ObjectTypeColumn, ObjectIdColumn },
                Left = left,
                Right = right,
                Expression = new ScalarFunction()
                {
                    ExtensionUri = FunctionsBoolean.Uri,
                    ExtensionName = FunctionsBoolean.And,
                    Arguments = new List<Expression>()
                    {
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = UserTypeColumn // Field 0 is the user type field of the left one
                                    }
                                },
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = left.OutputLength + UserTypeColumn // User type field in the right
                                    }
                                }
                            }
                        },
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = UserIdColumn // Field 0 is the user type field of the left one
                                    }
                                },
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = left.OutputLength + UserIdColumn // User type field in the right
                                    }
                                }
                            }
                        }
                    }
                }
            };
        }

        private static JoinRelation CreateJoinRelationUserTypeWildcard(Relation left, Relation right, bool wildcardLeft)
        {
            List<int>? emitList = default;

            if (wildcardLeft)
            {
                emitList = new List<int>()
                {
                    left.OutputLength + UserTypeColumn,
                    left.OutputLength + UserIdColumn,
                    left.OutputLength + UserRelationColumn,
                    RelationColumn,
                    ObjectTypeColumn,
                    ObjectIdColumn
                };
            }
            else
            {
                emitList = new List<int>()
                {
                    UserTypeColumn,
                    UserIdColumn,
                    UserRelationColumn,
                    RelationColumn,
                    ObjectTypeColumn,
                    ObjectIdColumn
                };
            }

            return new JoinRelation()
            {
                Emit = emitList,
                Left = left,
                Right = right,
                Type = JoinType.Inner,
                // l.user_type = r.user_type AND l.user_id = r.user_id
                Expression = new ScalarFunction()
                {
                    ExtensionUri = FunctionsBoolean.Uri,
                    ExtensionName = FunctionsBoolean.And,
                    Arguments = new List<Expression>()
                    {
                        // l.user_type = r.user_type
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = UserTypeColumn
                                    }
                                },
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = left.OutputLength + UserTypeColumn
                                    }
                                },
                            }
                        },
                        // l.user_id = '*'
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = wildcardLeft ? FunctionsComparison.Equal : FunctionsComparison.NotEqual,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = UserIdColumn
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = "*"
                                }
                            }
                        },
                        // r.user_id != '*'
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = wildcardLeft ? FunctionsComparison.NotEqual : FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = left.OutputLength + UserIdColumn
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = "*"
                                }
                            }
                        }
                    }
                }
            };
        }

        /// <summary>
        /// Creates a join relation used when there is a user relation.
        /// Example:
        /// <group:1#member, can_read, doc:1>
        /// <user:1, member, group:1>
        /// 
        /// This returns <user:1, can_read, doc:1>
        /// </summary>
        /// <param name="objectRelation"></param>
        /// <param name="userRelation"></param>
        /// <returns></returns>
        private static JoinRelation CreateUserRelationJoin(Relation objectRelation, Relation userRelation)
        {
            return new JoinRelation()
            {
                Emit = new List<int>() 
                {
                    // User is taken from the right side
                    objectRelation.OutputLength + UserTypeColumn,
                    objectRelation.OutputLength + UserIdColumn, 
                    objectRelation.OutputLength + UserRelationColumn, 
                    // Keep relation name
                    RelationColumn, 
                    // Keep object
                    ObjectTypeColumn, 
                    ObjectIdColumn 
                },
                Left = objectRelation,
                Right = userRelation,
                Expression = new ScalarFunction()
                {
                    ExtensionUri = FunctionsBoolean.Uri,
                    ExtensionName = FunctionsBoolean.And,
                    Arguments = new List<Expression>()
                    {
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = UserTypeColumn
                                    }
                                },
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = objectRelation.OutputLength + ObjectTypeColumn
                                    }
                                }
                            }
                        },
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = UserIdColumn
                                    }
                                },
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = objectRelation.OutputLength + ObjectIdColumn
                                    }
                                }
                            }
                        }
                    }
                }
            };
        }
    }
}
