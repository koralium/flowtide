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

using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using OpenFga.Sdk.Model;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    internal partial class FlowtideOpenFgaModelParser
    {
        private sealed class TypeReference
        {
            public required string Type { get; set; }

            public required string Relation { get; set; }

            public override bool Equals(object? obj)
            {
                if (obj is TypeReference other)
                {
                    return other.Type == Type && other.Relation == Relation;
                }
                return false;
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(Type, Relation);
            }
        }

        private sealed class TupleReference
        {
            public required string UserType { get; set; }

            public required string Relation { get; set; }

            public required string ObjectType { get; set; }

            public override bool Equals(object? obj)
            {
                if (obj is TupleReference other)
                {
                    return other.UserType == UserType && other.Relation == Relation && ObjectType == other.ObjectType;
                }
                return false;
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(UserType, Relation, ObjectType);
            }
        }

        private readonly AuthorizationModel authorizationModel;
        private readonly HashSet<string> _stopTypes;
        private HashSet<TypeReference> typeReferences = new HashSet<TypeReference>();
        private HashSet<TypeReference> handledTypeReferences = new HashSet<TypeReference>();
        private Relation? readRelation;

        private const int UserTypeColumn = 0;
        private const int UserIdColumn = 1;
        private const int UserRelationColumn = 2;
        private const int RelationColumn = 3;
        private const int ObjectTypeColumn = 4;
        private const int ObjectIdColumn = 5;

        private sealed class Result
        {
            public required List<TypeDefinition> ResultTypes { get; set; }

            public required Relation Relation { get; set; }

            public required List<ResultUserType> ResultUserType { get; set; }

            /// <summary>
            /// Hashset of types that are not computed.
            /// This is used to add filters to the loop to not send them again.
            /// </summary>
            public required HashSet<TupleReference> NonComputedTypes { get; set; }
        }

        public sealed class ResultUserType
        {
            public required string TypeName { get; set; }

            public bool Wildcard { get; set; }

            public override bool Equals(object? obj)
            {
                if (obj is ResultUserType other)
                {
                    return other.TypeName == TypeName && other.Wildcard == Wildcard;
                }
                return false;
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(TypeName, Wildcard);
            }
        }

        public FlowtideOpenFgaModelParser(AuthorizationModel authorizationModel, HashSet<string> stopTypes)
        {
            this.authorizationModel = authorizationModel;
            _stopTypes = stopTypes;
        }

        public Plan Parse(string type, string relation, string inputTableName)
        {
            readRelation = new IterationReferenceReadRelation()
            {
                ReferenceOutputLength = 6,
                IterationName = "auth"
            };

            typeReferences.Add(new TypeReference() { Type = type, Relation = relation });
            var (loopPlan, nonComputedTuples, userTypes) = Start(authorizationModel);

            var readRel = new ReadRelation()
            {
                NamedTable = new NamedTable()
                {
                    Names = new List<string>() { inputTableName }
                },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { 
                        "user_type", 
                        "user_id", 
                        "user_relation", 
                        "relation", 
                        "object_type", 
                        "object_id" 
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

            Expression? skipIterateCondition = default;
            if (nonComputedTuples.Count > 0)
            {
                List<Expression> orExpressions = new List<Expression>();
                foreach (var tuple in nonComputedTuples)
                {
                    // (user_type = 'type' AND relation = 'relation' AND object_type = 'object_type')
                    var func = new ScalarFunction()
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
                                    new StringLiteral()
                                    {
                                        Value = tuple.UserType
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
                                            Field = RelationColumn
                                        }
                                    },
                                    new StringLiteral()
                                    {
                                        Value = tuple.Relation
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
                                        Value = tuple.ObjectType
                                    }
                                }
                            }
                        }
                    };
                    orExpressions.Add(func);
                }

                if (orExpressions.Count > 1)
                {
                    var orFunc = new ScalarFunction()
                    {
                        ExtensionUri = FunctionsBoolean.Uri,
                        ExtensionName = FunctionsBoolean.Or,
                        Arguments = orExpressions
                    };
                    skipIterateCondition = orFunc;
                }
                else
                {
                    skipIterateCondition = orExpressions[0];
                }
            }

            var iterationRel = new IterationRelation()
            {
                IterationName = "auth",
                LoopPlan = loopPlan,
                Input = readRel,
                SkipIterateCondition = skipIterateCondition
            };

            var filterRel = CreateRelationTypeFilter(iterationRel, relation, type, userTypes);

            var rootRel = new RootRelation()
            {
                Input = filterRel,
                Names = new List<string>() 
                { 
                    "user_type", 
                    "user_id", 
                    "user_relation", 
                    "relation", 
                    "object_type", 
                    "object_id" 
                }
            };

            var plan = new Plan()
            {
                Relations = new List<Relation>() { rootRel }
            };
            return plan;
        }

        private (Relation, HashSet<TupleReference>, HashSet<ResultUserType>) Start(AuthorizationModel authorizationModel)
        {
            var union = new SetRelation()
            {
                Inputs = new List<Relation>(),
                Operation = SetOperation.UnionAll
            };
            HashSet<TupleReference> nonComputedTuples = new HashSet<TupleReference>();
            HashSet<ResultUserType> resultUserTypes = new HashSet<ResultUserType>();
            while (true)
            {
                if (typeReferences.Count == handledTypeReferences.Count)
                {
                    break;
                }
                var t = typeReferences.Except(handledTypeReferences).First();
                handledTypeReferences.Add(t);
                var typeDefinition = authorizationModel.TypeDefinitions.Find(x => x.Type.Equals(t.Type, StringComparison.OrdinalIgnoreCase));

                if (typeDefinition == null)
                {
                    throw new InvalidOperationException($"Type {t.Type} not found in authorization model");
                }
                if (typeDefinition.Relations == null)
                {
                    throw new InvalidOperationException($"Type {t.Type} has no relations defined");
                }
                if (!typeDefinition.Relations.TryGetValue(t.Relation, out var relationDefinition))
                {
                    throw new InvalidOperationException($"Relation {t.Relation} not found in type {t.Type}");
                }
                var result = VisitRelationDefinition(relationDefinition, t.Relation, typeDefinition);

                foreach (var nonComputedType in result.NonComputedTypes)
                {
                    nonComputedTuples.Add(nonComputedType);
                }
                foreach(var userType in result.ResultUserType)
                {
                    resultUserTypes.Add(userType);
                }

                if (result.Relation is SetRelation setRel)
                {
                    foreach (var input in setRel.Inputs)
                    {
                        union.Inputs.Add(input);
                    }
                }
                else
                {
                    union.Inputs.Add(result.Relation);
                }
            }

            return (union, nonComputedTuples, resultUserTypes);
        }

        private Result VisitRelationDefinition(Userset relationDefinition, string relationName, TypeDefinition typeDefinition)
        {
            if (relationDefinition.This != null)
            {
                return VisitThis(relationName, typeDefinition);
            }
            if (relationDefinition.Union != null)
            {
                return VisitUnion(relationDefinition.Union, relationName, typeDefinition);
            }
            if (relationDefinition.ComputedUserset != null)
            {
                return VisitComputedUserset(relationDefinition.ComputedUserset, relationName, typeDefinition);
            }
            if (relationDefinition.TupleToUserset != null)
            {
                return VisitTupleToUserset(relationDefinition.TupleToUserset, relationName, typeDefinition);
            }
            if (relationDefinition.Intersection != null)
            {
                return VisitIntersection(relationDefinition.Intersection, relationName, typeDefinition);
            }
            throw new NotImplementedException();
        }

        private Result VisitIntersection(Usersets userset, string relationName, TypeDefinition typeDefinition)
        {
            var first = userset.Child[0];
            var firstRel = VisitRelationDefinition(first, relationName, typeDefinition);

            var rootRel = firstRel.Relation;
            var resultTypes = firstRel.ResultUserType.ToHashSet();
            HashSet<TupleReference> nonComputedTypes = new HashSet<TupleReference>(firstRel.NonComputedTypes);
            for (int i = 1; i < userset.Child.Count; i++)
            {
                var subRel = VisitRelationDefinition(userset.Child[i], relationName, typeDefinition);

                foreach (var t in subRel.NonComputedTypes)
                {
                    nonComputedTypes.Add(t);
                }

                var comibedTypes = new HashSet<ResultUserType>(resultTypes);
                foreach (var t in subRel.ResultUserType)
                {
                    comibedTypes.Add(t);
                }
                bool containsWildcard = comibedTypes.Any(x => x.Wildcard);

                // No wildcard, an inner join can be used.
                if (!containsWildcard)
                {
                    // INNER JOIN ON l.user_type = r.user_type AND l.user_id = r.user_id
                    rootRel = CreateJoinRelationUserTypeAndId(rootRel, subRel.Relation);
                }
                else
                {
                    // Wildcard, left join must be used with a filter condition.
                    // Must have a projection that tries to select the user field that is not a wildcard.
                    // But if both are wildcard, wildcard is returned.

                    // Set operation that merges together the different results.
                    SetRelation setRelation = new SetRelation()
                    {
                        Inputs = new List<Relation>(),
                        Operation = SetOperation.UnionAll
                    };

                    // First join checks if the user field is equal with no wildcard check
                    var joinEqual = CreateJoinRelationUserTypeAndId(rootRel, subRel.Relation);

                    setRelation.Inputs.Add(joinEqual);

                    var leftHasWildcard = resultTypes.Any(x => x.Wildcard);
                    var rightHasWildcard = subRel.ResultUserType.Exists(x => x.Wildcard);

                    if (leftHasWildcard)
                    {
                        // This join takes the user from the right input since it only looks for wildcards on left side.
                        var leftWildcardJoin = CreateJoinRelationUserTypeWildcard(rootRel, subRel.Relation, true);

                        setRelation.Inputs.Add(leftWildcardJoin);
                    }

                    if (rightHasWildcard)
                    {
                        // Do same as for left side but instead look if right side user_id is * but not left side.
                        var rightWildcardJoin = CreateJoinRelationUserTypeWildcard(rootRel, subRel.Relation, false);

                        setRelation.Inputs.Add(rightWildcardJoin);
                    }

                    resultTypes = comibedTypes;
                    rootRel = setRelation;
                }
            }

            return new Result()
            {
                Relation = rootRel,
                ResultTypes = firstRel.ResultTypes,
                ResultUserType = resultTypes.ToList(),
                NonComputedTypes = nonComputedTypes
            };
        }

        private (List<ResultUserType>, bool isDirect) GetTupleToUsersetUserType(ObjectRelation objectRelation, TypeDefinition typeDefinition)
        {
            if (typeDefinition.Relations == null)
            {
                throw new InvalidOperationException($"Type {typeDefinition.Type} has no relations defined");
            }
            if (objectRelation.Relation == null)
            {
                throw new InvalidOperationException("Relation is null in object relation.");
            }
            if (!typeDefinition.Relations.TryGetValue(objectRelation.Relation, out var relationDef))
            {
                throw new InvalidOperationException($"Relation {objectRelation.Relation} not found in type {typeDefinition.Type}");
            }
            return GetUserType(relationDef, objectRelation.Relation, typeDefinition, new HashSet<TypeReference>());
        }

        private List<ResultUserType> GetTupleToUsersetUserType(ObjectRelation objectRelation, TypeDefinition typeDefinition, HashSet<TypeReference> visitedRelations)
        {
            if (typeDefinition.Relations == null)
            {
                throw new InvalidOperationException($"Type {typeDefinition.Type} has no relations defined");
            }
            if (objectRelation.Relation == null)
            {
                throw new InvalidOperationException("Relation is null in object relation.");
            }
            if (!typeDefinition.Relations.TryGetValue(objectRelation.Relation, out var relationDef))
            {
                throw new InvalidOperationException($"Relation {objectRelation.Relation} not found in type {typeDefinition.Type}");
            }
            return GetUserType(relationDef, objectRelation.Relation, typeDefinition, visitedRelations).Item1;
        }

        private (List<ResultUserType>, bool isDirect) GetUserType(Userset userset, string relationName, TypeDefinition typeDefinition, HashSet<TypeReference> visitedRelations)
        {
            if (userset.This != null)
            {
                return (GetUserTypeFromThis(relationName, typeDefinition, visitedRelations), true);
            }
            else if (userset.Union != null)
            {
                return (GetUserTypeFromUnion(userset.Union, relationName, typeDefinition, visitedRelations), false);
            }
            else if (userset.ComputedUserset != null)
            {
                return (GetUserTypeFromComputedUserset(userset.ComputedUserset, typeDefinition, visitedRelations), false);
            }
            else if (userset.TupleToUserset != null)
            {
                return (GetUserTypeFromTupleToUserset(userset.TupleToUserset, relationName, typeDefinition, visitedRelations), false);
            }
            throw new NotImplementedException();
        }

        private List<ResultUserType> GetUserTypeFromTupleToUserset(TupleToUserset tupleToUserset, string toRelationName, TypeDefinition typeDefinition, HashSet<TypeReference> visitedRelations)
        {
            if (visitedRelations.Contains(new TypeReference() { Type = typeDefinition.Type, Relation = toRelationName }))
            {
                return new List<ResultUserType>();
            }
            visitedRelations.Add(new TypeReference() { Type = typeDefinition.Type, Relation = toRelationName });
            var tuplesetResult = VisitTupleset(tupleToUserset.Tupleset, typeDefinition);

            if (tuplesetResult.ResultTypes.Count > 1)
            {
                throw new InvalidOperationException("At this time only 1 type is allowed for tuple to userset");
            }
            var resultType = tuplesetResult.ResultTypes[0];
            var userTypes = GetTupleToUsersetUserType(tupleToUserset.ComputedUserset, resultType, visitedRelations);

            return userTypes;
        }

        private List<ResultUserType> GetUserTypeFromComputedUserset(ObjectRelation objectRelation, TypeDefinition typeDefinition, HashSet<TypeReference> visitedRelations)
        {
            if (typeDefinition.Relations == null)
            {
                throw new InvalidOperationException($"Type {typeDefinition.Type} has no relations defined");
            }
            if (objectRelation.Relation == null)
            {
                throw new InvalidOperationException("Relation is null in object relation.");
            }
            if (!typeDefinition.Relations.TryGetValue(objectRelation.Relation, out var relationDef))
            {
                throw new InvalidOperationException($"Relation {objectRelation.Relation} not found in type {typeDefinition.Type}");
            }
            return GetUserType(relationDef, objectRelation.Relation, typeDefinition, visitedRelations).Item1;
        }

        private List<ResultUserType> GetUserTypeFromUnion(Usersets usersets, string relationName, TypeDefinition typeDefinition, HashSet<TypeReference> visitedRelations)
        {
            HashSet<ResultUserType> resultUserTypes = new HashSet<ResultUserType>();
            foreach (var child in usersets.Child)
            {
                var types = GetUserType(child, relationName, typeDefinition, visitedRelations).Item1;
                foreach (var t in types)
                {
                    resultUserTypes.Add(t);
                }
            }
            return resultUserTypes.ToList();
        }

        private static List<ResultUserType> GetUserTypeFromThis(string relationName, TypeDefinition typeDefinition, HashSet<TypeReference> visitedRelations)
        {
            visitedRelations.Add(new TypeReference() { Type = typeDefinition.Type, Relation = relationName });
            if (typeDefinition.Metadata == null)
            {
                throw new InvalidOperationException($"Type {typeDefinition.Type} has no metadata defined");
            }
            if (typeDefinition.Metadata.Relations == null)
            {
                throw new InvalidOperationException($"Type {typeDefinition.Type} has no relations defined");
            }
            if (!typeDefinition.Metadata.Relations.TryGetValue(relationName, out var thisRelation))
            {
                throw new InvalidOperationException($"Relation {relationName} not found in type {typeDefinition.Type}");
            }
            if (thisRelation.DirectlyRelatedUserTypes == null)
            {
                throw new InvalidOperationException($"Relation {relationName} has no directly related user types defined");
            }

            List<ResultUserType> userTypes = new List<ResultUserType>();
            foreach (var t in thisRelation.DirectlyRelatedUserTypes)
            {
                if (t.Relation != null)
                {
                    throw new NotImplementedException();
                }
                userTypes.Add(new ResultUserType() { TypeName = t.Type, Wildcard = t.Wildcard != null });
            }
            return userTypes;
        }

        private Result VisitTupleToUserset(TupleToUserset tupleToUserset, string toRelationName, TypeDefinition typeDefinition)
        {
            var tuplesetResult = VisitTupleset(tupleToUserset.Tupleset, typeDefinition);

            if (tuplesetResult.ResultTypes.Count > 1)
            {
                throw new InvalidOperationException("At this time only 1 type is allowed for tuple to userset");
            }
            var resultType = tuplesetResult.ResultTypes[0];

            if (_stopTypes.Contains(resultType.Type))
            {
                // Add a project to change the relation
                var stopProjectRel = new ProjectRelation()
                {
                    Input = tuplesetResult.Relation,
                    Expressions = new List<Expression>()
                    {
                        new StringLiteral()
                        {
                            Value = toRelationName
                        }
                    },
                    Emit = new List<int>()
                    {
                        UserTypeColumn,
                        UserIdColumn,
                        UserRelationColumn,
                        tuplesetResult.Relation.OutputLength, // The new expression is added as relation name
                        ObjectTypeColumn,
                        ObjectIdColumn
                    }
                };

                return new Result()
                {
                    Relation = stopProjectRel,
                    ResultTypes = new List<TypeDefinition>() { typeDefinition },
                    ResultUserType = tuplesetResult.ResultUserType,
                    NonComputedTypes = tuplesetResult.NonComputedTypes
                };
            }

            var (userTypes, isDirect) = GetTupleToUsersetUserType(tupleToUserset.ComputedUserset, resultType);

            if (tupleToUserset.ComputedUserset.Relation == null)
            {
                throw new InvalidOperationException("Relation is null in computed userset.");
            }

            if (!isDirect)
            {
                typeReferences.Add(new TypeReference() { Type = resultType.Type, Relation = tupleToUserset.ComputedUserset.Relation });
            }

            if (readRelation == null)
            {
                throw new InvalidOperationException("Read relation is null");
            }

            var filterRel = CreateRelationTypeFilter(readRelation, tupleToUserset.ComputedUserset.Relation, resultType.Type);

            var joinRel = new JoinRelation()
            {
                Left = tuplesetResult.Relation,
                Right = filterRel,
                Type = JoinType.Inner,
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
                                        Field = tuplesetResult.Relation.OutputLength + ObjectTypeColumn // Field 0 is the user field of the left one
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
                                        Field = tuplesetResult.Relation.OutputLength + ObjectIdColumn // Object id from right
                                    }
                                }
                            }
                        }
                    }
                }
            };

            var projectRel = new ProjectRelation()
            {
                Input = joinRel,
                Expressions = new List<Expression>()
                {
                    new StringLiteral()
                    {
                        Value = toRelationName
                    }
                },
                Emit = new List<int>()
                {
                    tuplesetResult.Relation.OutputLength + UserTypeColumn,
                    tuplesetResult.Relation.OutputLength + UserIdColumn,
                    tuplesetResult.Relation.OutputLength + UserRelationColumn,
                    joinRel.OutputLength, // The new expression is added as relation name
                    ObjectTypeColumn, // Keep the object type from left
                    ObjectIdColumn // Keep the object id from left
                }
            };

            return new Result()
            {
                Relation = projectRel,
                ResultTypes = new List<TypeDefinition>() { typeDefinition },
                ResultUserType = userTypes,
                // Clear since this is a computed relation.
                NonComputedTypes = new HashSet<TupleReference>()
            };
        }

        private Result VisitTupleset(ObjectRelation tupleSet, TypeDefinition typeDefinition)
        {
            if (typeDefinition.Relations == null)
            {
                throw new InvalidOperationException($"Type {typeDefinition.Type} has no relations defined");
            }
            if (tupleSet.Relation == null)
            {
                throw new InvalidOperationException("Relation is null in object relation.");
            }
            if (!typeDefinition.Relations.TryGetValue(tupleSet.Relation, out var relationDef))
            {
                throw new InvalidOperationException($"Relation {tupleSet.Relation} not found in type {typeDefinition.Type}");
            }
            return VisitRelationDefinition(relationDef, tupleSet.Relation, typeDefinition);
        }

        private Result VisitThis(string relationName, TypeDefinition typeDefinition)
        {
            if (typeDefinition.Metadata == null)
            {
                throw new InvalidOperationException($"Type {typeDefinition.Type} has no metadata defined");
            }
            if (typeDefinition.Metadata.Relations == null)
            {
                throw new InvalidOperationException($"Type {typeDefinition.Type} has no relations defined");
            }
            if (!typeDefinition.Metadata.Relations.TryGetValue(relationName, out var thisRelation))
            {
                throw new InvalidOperationException($"Relation {relationName} not found in type {typeDefinition.Type}");
            }
            if (thisRelation.DirectlyRelatedUserTypes == null)
            {
                throw new InvalidOperationException($"Relation {relationName} has no directly related user types defined");
            }
            if (readRelation == null)
            {
                throw new InvalidOperationException("Read relation is null");
            }

            // Contains the relations that builds up this data relation.
            List<Relation> relations = new List<Relation>();
            HashSet<TupleReference> nonModifiedTuples = new HashSet<TupleReference>();
            List<TypeDefinition> types = new List<TypeDefinition>();
            var userTypes = new List<ResultUserType>();
            foreach (var t in thisRelation.DirectlyRelatedUserTypes)
            {
                var typeDef = authorizationModel.TypeDefinitions.Find(x => x.Type == t.Type);
                if (typeDef == null)
                {
                    throw new InvalidOperationException();
                }

                if (t.Relation != null)
                {
                    // if there is a relation, that data must be joined with the existing data.
                    var typeResult = VisitUserRelation(t, relationName, typeDef, typeDefinition);
                    relations.Add(typeResult.Relation);
                    foreach(var nonModifiedTuple in typeResult.NonComputedTypes)
                    {
                        nonModifiedTuples.Add(nonModifiedTuple);
                    }
                    userTypes.AddRange(typeResult.ResultUserType);
                }
                else
                {
                    var typeResult = VisitDirectUser(t, typeDef, relationName, typeDefinition);
                    relations.Add(typeResult.Relation);
                    foreach (var nonModifiedTuple in typeResult.NonComputedTypes)
                    {
                        nonModifiedTuples.Add(nonModifiedTuple);
                    }
                    userTypes.AddRange(typeResult.ResultUserType);
                }

                types.Add(typeDef);
            }

            Relation? rel = default;
            if (relations.Count > 1)
            {
                rel = new SetRelation()
                {
                    Inputs = relations,
                    Operation = SetOperation.UnionAll
                };
            }
            else
            {
                rel = relations[0];
            }

            return new Result() { Relation = rel, ResultTypes = types, ResultUserType = userTypes, NonComputedTypes = nonModifiedTuples };
        }

        private Result VisitDirectUser(RelationReference relationReference, TypeDefinition referenceType, string relationName, TypeDefinition objectType)
        {
            var filterRel = new FilterRelation()
            {
                Input = readRelation,
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
                                    Value = referenceType.Type
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
                                    Value = relationName
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
                                    Value = objectType.Type
                                }
                            }
                        }
                    }
                }
            };

            return new Result()
            {
                NonComputedTypes = new HashSet<TupleReference>()
                {
                    new TupleReference()
                    {
                        UserType = referenceType.Type,
                        Relation = relationName,
                        ObjectType = objectType.Type
                    }
                },
                Relation = filterRel,
                ResultTypes = new List<TypeDefinition>() { referenceType },
                ResultUserType = new List<ResultUserType>()
                {
                    new ResultUserType()
                    {
                        TypeName = relationReference.Type,
                        Wildcard = relationReference.Wildcard != null
                    }
                }
            };
        }

        private Result VisitUserRelation(RelationReference relationReference, string toRelationName, TypeDefinition referenceType, TypeDefinition objectType)
        {
            if (referenceType.Relations == null)
            {
                throw new InvalidOperationException($"Type {referenceType.Type} has no relations defined");
            }
            if (relationReference.Relation == null)
            {
                throw new InvalidOperationException("Relation is null in relation reference.");
            }
            if (!referenceType.Relations.TryGetValue(relationReference.Relation, out var relationDef))
            {
                throw new InvalidOperationException($"Relation {relationReference.Relation} not found in type {referenceType.Type}");
            }

            // Filter relation for tuples that are of the correct type and relation
            var filterRel = CreateUserRelationFilter(
                readRelation, 
                relationReference.Type, 
                relationReference.Relation, 
                toRelationName, 
                objectType.Type);

            // Get tuples of the referenced relation
            var result = VisitRelationDefinition(relationDef, relationReference.Relation, referenceType);

            // Join the filter relation with the relation of the referenced relation
            var joinRelation = CreateUserRelationJoin(filterRel, result.Relation);

            return new Result()
            {
                NonComputedTypes = new HashSet<TupleReference>(),
                Relation = joinRelation,
                ResultTypes = result.ResultTypes,
                ResultUserType = result.ResultUserType
            };
        }

        private Result VisitComputedUserset(ObjectRelation objectRelation, string toRelationName, TypeDefinition typeDefinition)
        {
            if (typeDefinition.Relations == null)
            {
                throw new InvalidOperationException($"Type {typeDefinition.Type} has no relations defined");
            }
            if (objectRelation.Relation == null)
            {
                throw new InvalidOperationException("Relation is null in object relation.");
            }
            if (!typeDefinition.Relations.TryGetValue(objectRelation.Relation, out var relationDef))
            {
                throw new InvalidOperationException($"Relation {objectRelation.Relation} not found in type {typeDefinition.Type}");
            }
            var relation = VisitRelationDefinition(relationDef, objectRelation.Relation, typeDefinition);

            var projectRelation = new ProjectRelation()
            {
                Input = relation.Relation,
                Expressions = new List<Expression>()
                {
                    new StringLiteral()
                    {
                        Value = toRelationName
                    }
                },
                Emit = new List<int>()
                {
                    UserTypeColumn,
                    UserIdColumn,
                    UserRelationColumn,
                    relation.Relation.OutputLength, // The new expression is added as relation name
                    ObjectTypeColumn,
                    ObjectIdColumn
                }
            };

            return new Result()
            {
                Relation = projectRelation,
                ResultTypes = relation.ResultTypes,
                ResultUserType = new List<ResultUserType>(),
                // Clear non computed types since this is a computed relation.
                // No original tuples will be returned
                NonComputedTypes = new HashSet<TupleReference>()
            };
        }

        private Result VisitUnion(Usersets usersets, string relationName, TypeDefinition typeDefinition)
        {
            var relation = new SetRelation()
            {
                Inputs = new List<Relation>(),
                Operation = SetOperation.UnionAll
            };
            HashSet<TupleReference> nonComputedTuples = new HashSet<TupleReference>();
            foreach (var child in usersets.Child)
            {
                var subRel = VisitRelationDefinition(child, relationName, typeDefinition);
                relation.Inputs.Add(subRel.Relation);
                foreach (var t in subRel.NonComputedTypes)
                {
                    nonComputedTuples.Add(t);
                }
            }
            return new Result()
            {
                Relation = relation,
                ResultTypes = new List<TypeDefinition>() { typeDefinition },
                ResultUserType = new List<ResultUserType>(),
                NonComputedTypes = nonComputedTuples
            };
        }
    }
}
