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

using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using FlowtideDotNet.Zanzibar.QueryPlanner.Models;

namespace FlowtideDotNet.Zanzibar.QueryPlanner
{
    public class ZanzibarToFlowtideVisitor : ZanzibarVisitor<Relation, object?>
    {
        private const int UserTypeColumn = 0;
        private const int UserIdColumn = 1;
        private const int UserRelationColumn = 2;
        private const int RelationColumn = 3;
        private const int ObjectTypeColumn = 4;
        private const int ObjectIdColumn = 5;

        private readonly string inputTableName;
        private readonly string subjectTypeColumnName;
        private readonly string subjectIdColumnName;
        private readonly string subjectRelationColumnName;
        private readonly string relationColumnName;
        private readonly string resourceTypeColumnName;
        private readonly string resourceIdColumnName;

        private Dictionary<string, int> _iterationOutputLengths;
        private Dictionary<string, List<IterationReferenceReadRelation>> _iterationReferenceRelations;

        public ZanzibarToFlowtideVisitor(
            string inputTableName,
            string subjectTypeColumnName,
            string subjectIdColumnName,
            string subjectRelationColumnName,
            string relationColumnName,
            string resourceTypeColumnName,
            string resourceIdColumnName)
        {
            this.inputTableName = inputTableName;
            this.subjectTypeColumnName = subjectTypeColumnName;
            this.subjectIdColumnName = subjectIdColumnName;
            this.subjectRelationColumnName = subjectRelationColumnName;
            this.relationColumnName = relationColumnName;
            this.resourceTypeColumnName = resourceTypeColumnName;
            this.resourceIdColumnName = resourceIdColumnName;
            _iterationOutputLengths = new Dictionary<string, int>();
            _iterationReferenceRelations = new Dictionary<string, List<IterationReferenceReadRelation>>();
        }

        private ReadRelation GetReadRelation()
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable()
                {
                    Names = new List<string>() { inputTableName }
                },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() {
                        subjectTypeColumnName,
                        subjectIdColumnName,
                        subjectRelationColumnName,
                        relationColumnName,
                        resourceTypeColumnName,
                        resourceIdColumnName
                    },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                        {
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType()
                        }
                    }
                }
            };
        }

        public override Relation VisitZanzibarChangeRelationName(ZanzibarChangeRelationName changeRelationName, object? state)
        {
            var input = changeRelationName.Input.Accept(this, state);

            return new ProjectRelation()
            {
                Input = input,
                Expressions = new List<Expression>()
                {
                    new StringLiteral()
                    {
                        Value = changeRelationName.NewRelationName
                    }
                },
                Emit = new List<int>()
                {
                    UserTypeColumn,
                    UserIdColumn,
                    UserRelationColumn,
                    input.OutputLength, // The new expression is added as relation name
                    ObjectTypeColumn,
                    ObjectIdColumn
                }
            };
        }

        public override Relation VisitZanzibarJoinIntersectWildcard(ZanzibarJoinIntersectWildcard joinIntersectWildcard, object? state)
        {
            List<int>? emitList = default;

            var left = joinIntersectWildcard.Left.Accept(this, state);
            var right = joinIntersectWildcard.Right.Accept(this, state);
            var wildcardLeft = joinIntersectWildcard.LeftWildcard;
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
                        // l.object_type = r.object_type
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
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = left.OutputLength + ObjectTypeColumn
                                    }
                                },
                            }
                        },
                        // l.object_id = r.object_id
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
                                        Field = ObjectIdColumn
                                    }
                                },
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = left.OutputLength + ObjectIdColumn
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

        public override Relation VisitZanzibarJoinOnUserTypeId(ZanzibarJoinOnUserTypeId joinOnUserTypeId, object? state)
        {
            var left = joinOnUserTypeId.Left.Accept(this, state);
            var right = joinOnUserTypeId.Right.Accept(this, state);

            return new JoinRelation()
            {
                Emit = new List<int>() { UserTypeColumn, UserIdColumn, UserRelationColumn, RelationColumn, ObjectTypeColumn, ObjectIdColumn },
                Left = left,
                Right = right,
                Type = JoinType.Inner,
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

        public override Relation VisitZanzibarJoinUserRelation(ZanzibarJoinUserRelation joinUserRelation, object? state)
        {
            var left = joinUserRelation.Left.Accept(this, state);
            var right = joinUserRelation.Right.Accept(this, state);
            return new JoinRelation()
            {
                Emit = new List<int>()
                {
                    // User is taken from the right side
                    left.OutputLength + UserTypeColumn,
                    left.OutputLength + UserIdColumn,
                    left.OutputLength + UserRelationColumn, 
                    // Keep relation name
                    RelationColumn, 
                    // Keep object
                    ObjectTypeColumn,
                    ObjectIdColumn
                },
                Left = left,
                Right = right,
                Type = JoinType.Inner,
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
                                        Field = left.OutputLength + ObjectTypeColumn
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
                                        Field = left.OutputLength + ObjectIdColumn
                                    }
                                }
                            }
                        }
                    }
                }
            };
        }

        public override Relation VisitZanzibarJoinUserToObject(ZanzibarJoinUserToObject joinUserToObject, object? state)
        {
            var left = joinUserToObject.Left.Accept(this, state);
            var right = joinUserToObject.Right.Accept(this, state);

            return new JoinRelation()
            {
                Left = left,
                Right = right,
                Type = JoinType.Inner,
                Emit = new List<int>()
                {
                    left.OutputLength + UserTypeColumn,
                    left.OutputLength + UserIdColumn,
                    left.OutputLength + UserRelationColumn,
                    left.OutputLength + RelationColumn, // The new expression is added as relation name
                    ObjectTypeColumn, // Keep the object type from left
                    ObjectIdColumn // Keep the object id from left
                },
                Expression = new ScalarFunction()
                {
                    ExtensionUri = FunctionsBoolean.Uri,
                    ExtensionName = FunctionsBoolean.And,
                    Arguments = new List<Expression>()
                    {
                        // user_type = object_type
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
                                        Field = UserTypeColumn //User type from left
                                    }
                                },
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = left.OutputLength + ObjectTypeColumn // Field 0 is the user field of the left one
                                    }
                                },
                            }
                        },
                        // user_id = object_id
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
                                        Field = UserIdColumn //User id from left
                                    }
                                },
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = left.OutputLength + ObjectIdColumn // Object id from right
                                    }
                                }
                            }
                        }
                    }
                }
            };
        }

        public override Relation VisitZanzibarLoop(ZanzibarLoop loop, object? state)
        {
            var input = loop.LoopRelation.Accept(this, state);

            var id = $"{loop.Type}_{loop.Relation}";
            _iterationOutputLengths.Add(id, input.OutputLength);

            if (_iterationReferenceRelations.TryGetValue(id, out var existingRelations))
            {
                foreach (var rel in existingRelations)
                {
                    rel.ReferenceOutputLength = input.OutputLength;
                }
            }

            return new IterationRelation()
            {
                LoopPlan = input,
                IterationName = $"{loop.Type}_{loop.Relation}",
                MaxIterations = 1000
            };
        }

        public override Relation VisitZanzibarReadLoop(ZanzibarReadLoop readLoop, object? state)
        {
            var id = $"{readLoop.Type}_{readLoop.Relation}";
            if (_iterationOutputLengths.TryGetValue(id, out var outputLength))
            {
                return new IterationReferenceReadRelation()
                {
                    IterationName = $"{readLoop.Type}_{readLoop.Relation}",
                    ReferenceOutputLength = outputLength
                };
            }
            else
            {
                if (!_iterationReferenceRelations.TryGetValue(id, out var existingRelations))
                {
                    existingRelations = new List<IterationReferenceReadRelation>();
                    _iterationReferenceRelations.Add(id, existingRelations);
                }
                var rel = new IterationReferenceReadRelation()
                {
                    IterationName = $"{readLoop.Type}_{readLoop.Relation}"
                };
                existingRelations.Add(rel);
                return rel;
            }
        }

        public override Relation VisitZanzibarReadUserAndObjectType(ZanzibarReadUserAndObjectType readUserAndObjectType, object? state)
        {
            return new FilterRelation()
            {
                Input = GetReadRelation(),
                Condition = new ScalarFunction()
                {
                    ExtensionName = FunctionsBoolean.And,
                    ExtensionUri = FunctionsBoolean.Uri,
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
                                new StringLiteral()
                                {
                                    Value = readUserAndObjectType.UserType
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
                                        Field = RelationColumn // Field 1 is the relation field
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = readUserAndObjectType.Relation
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
                                        Field = ObjectTypeColumn // Field 3 is the object type field
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = readUserAndObjectType.ObjectType
                                }
                            }
                        }
                    }
                }
            };
        }

        public override Relation VisitZanzibarReadUserRelation(ZanzibarReadUserRelation readUserRelation, object? state)
        {
            return new FilterRelation()
            {
                Input = GetReadRelation(),
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
                                    Value = readUserRelation.UserType
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
                                    Value = readUserRelation.UserRelation
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
                                    Value = readUserRelation.Relation
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
                                    Value = readUserRelation.ObjectType
                                }
                            }
                        }
                    }
                }
            };
        }

        public override Relation VisitZanzibarUnion(ZanzibarUnion union, object? state)
        {
            SetRelation setRelation = new SetRelation()
            {
                Inputs = new List<Relation>(),
                Operation = SetOperation.UnionAll
            };

            foreach (var input in union.Inputs)
            {
                setRelation.Inputs.Add(input.Accept(this, state));
            }

            return setRelation;
        }

        public override Relation VisitZanzibarRelationReference(ZanzibarRelationReference relationReference, object? state)
        {
            return new ReferenceRelation()
            {
                ReferenceOutputLength = 6,
                RelationId = relationReference.ReferenceId
            };
        }

        public override Relation VisitZanzibarCopyResourceToSubjectDistinct(ZanzibarCopyResourceToSubjectDistinct copyResourceToSubject, object? state)
        {
            var input = copyResourceToSubject.Input.Accept(this, state);

            var projectRel = new ProjectRelation()
            {
                Input = input,
                Emit = new List<int>()
                {
                    ObjectTypeColumn,
                    ObjectIdColumn,
                    UserRelationColumn,
                    RelationColumn,
                    ObjectTypeColumn,
                    ObjectIdColumn
                },
                Expressions = new List<Expression>()
            };

            return new SetRelation()
            {
                Inputs = [projectRel],
                Operation = SetOperation.UnionDistinct
            };
        }
    }
}
