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

namespace FlowtideDotNet.Connector.OpenFGA
{
    public class FlowtideOpenFgaModelParser
    {
        private class TypeReference
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

        private readonly AuthorizationModel authorizationModel;
        private HashSet<TypeReference> typeReferences = new HashSet<TypeReference>();
        private HashSet<TypeReference> handledTypeReferences = new HashSet<TypeReference>();
        private Relation? readRelation;


        private class Result
        {
            public required List<TypeDefinition> ResultTypes { get; set; }

            public required Relation Relation { get; set; }
        }

        public FlowtideOpenFgaModelParser(AuthorizationModel authorizationModel)
        {
            this.authorizationModel = authorizationModel;
        }

        public Plan Parse(string type, string relation, string inputTableName)
        {
            readRelation = new IterationReferenceReadRelation()
            {
                ReferenceOutputLength = 4,
                IterationName = "auth"
            };

            typeReferences.Add(new TypeReference() { Type = type, Relation = relation });
            var loopPlan = Start(authorizationModel);

            var readRel = new ReadRelation()
            {
                NamedTable = new NamedTable()
                {
                    Names = new List<string>() { inputTableName }
                },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "user", "relation", "object", "object_type" },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                        {
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType()
                        }
                    }
                }
            };

            var iterationRel = new IterationRelation()
            {
                IterationName = "auth",
                LoopPlan = loopPlan,
                Input = readRel
            };

            var filterRel = new FilterRelation()
            {
                Input = iterationRel,
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
                                        Field = 1 // Field 1 is the relation field
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
                                        Field = 3 // Field 3 is the object type field
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = type
                                }
                            }
                        }
                    }
                }
            };

            var rootRel = new RootRelation()
            {
                Input = filterRel,
                Names = new List<string>() { "user", "relation", "object", "object_type" }
            };

            var plan = new Plan()
            {
                Relations = new List<Relation>() { rootRel }
            };
            return plan;
        }

        public Relation Start(AuthorizationModel authorizationModel)
        {
            var union = new SetRelation()
            {
                Inputs = new List<Relation>(),
                Operation = SetOperation.UnionDistinct
            };
            while (true)
            {
                if (typeReferences.Count == handledTypeReferences.Count)
                {
                    break;
                }
                var t = typeReferences.Except(handledTypeReferences).FirstOrDefault();
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

            return union;
        }

        private Result VisitRelationDefinition(Userset relationDefinition, string relationName, TypeDefinition typeDefinition)
        {
            if (relationDefinition.This != null)
            {
                return VisitThis(relationDefinition, relationName, typeDefinition);
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
            throw new NotImplementedException();
        }

        private Result VisitTupleToUserset(TupleToUserset tupleToUserset, string toRelationName, TypeDefinition typeDefinition)
        {
            var tuplesetResult = VisitTupleset(tupleToUserset.Tupleset, typeDefinition);

            if (tuplesetResult.ResultTypes.Count > 1)
            {
                throw new InvalidOperationException("At this time only 1 type is allowed for tuple to userset");
            }
            var resultType = tuplesetResult.ResultTypes[0];
            typeReferences.Add(new TypeReference() { Type = resultType.Type, Relation = tupleToUserset.ComputedUserset.Relation });

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
                                        Field = 1 // Field 1 is the relation field
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = tupleToUserset.ComputedUserset.Relation
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
                                        Field = 3 // Field 3 is the object type field
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = resultType.Type
                                }
                            }
                        }
                    }
                }
            };

            var joinRel = new JoinRelation()
            {
                Left = tuplesetResult.Relation,
                Right = filterRel,
                Type = JoinType.Inner,
                Expression = new ScalarFunction()
                {
                    ExtensionUri = FunctionsComparison.Uri,
                    ExtensionName = FunctionsComparison.Equal,
                    Arguments = new List<Expression>()
                    {
                        new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment()
                            {
                                Field = 0 // Field 0 is the user field of the left one
                            }
                        },
                        new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment()
                            {
                                Field = 6 // Field 6 is the object field on right
                            }
                        }
                    }
                },
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
                    tuplesetResult.Relation.OutputLength, //Take the first column from right which is the user
                    joinRel.OutputLength, // The new expression is added as relation name
                    2, // Keep the object id from left
                    3 // Keep the object type from left
                }
            };

            return new Result() { Relation = projectRel, ResultTypes = new List<TypeDefinition>() { typeDefinition } };
        }

        private Result VisitTupleset(ObjectRelation tupleSet, TypeDefinition typeDefinition)
        {
            if (!typeDefinition.Relations.TryGetValue(tupleSet.Relation, out var relationDef))
            {
                throw new InvalidOperationException($"Relation {tupleSet.Relation} not found in type {typeDefinition.Type}");
            }
            return VisitRelationDefinition(relationDef, tupleSet.Relation, typeDefinition);
        }

        private Result VisitThis(Userset userset, string relationName, TypeDefinition typeDefinition)
        {
            if (!typeDefinition.Metadata.Relations.TryGetValue(relationName, out var thisRelation))
            {
                throw new InvalidOperationException($"Relation {relationName} not found in type {typeDefinition.Type}");
            }


            // Create a filter that checks that the relation name is equal and the type is equal to the expected type.
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
                                        Field = 1 // Field 1 is the relation field
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
                                        Field = 3 // Field 3 is the object type field
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = typeDefinition.Type
                                }
                            }
                        }
                    }
                }
            };

            List<TypeDefinition> types = new List<TypeDefinition>();

            foreach (var t in thisRelation.DirectlyRelatedUserTypes)
            {
                var typeDef = authorizationModel.TypeDefinitions.Find(x => x.Type == t.Type);
                if (typeDef == null)
                {
                    throw new InvalidOperationException();
                }
                types.Add(typeDef);
            }

            return new Result() { Relation = filterRel, ResultTypes = types };
        }

        private Result VisitComputedUserset(ObjectRelation objectRelation, string toRelationName, TypeDefinition typeDefinition)
        {
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
                    0,
                    4, // The new expression is added as relation name
                    2,
                    3
                }
            };

            return new Result() { Relation = projectRelation, ResultTypes = relation.ResultTypes };
        }

        private Result VisitUnion(Usersets usersets, string relationName, TypeDefinition typeDefinition)
        {
            var relation = new SetRelation()
            {
                Inputs = new List<Relation>(),
                Operation = SetOperation.UnionDistinct
            };

            foreach (var child in usersets.Child)
            {
                var subRel = VisitRelationDefinition(child, relationName, typeDefinition);
                relation.Inputs.Add(subRel.Relation);
            }
            return new Result() { Relation = relation, ResultTypes = new List<TypeDefinition>() { typeDefinition } };//new Result() { ResultTypes = new List<TypeDefinition>() { typeDefinition } };
        }
    }
}
