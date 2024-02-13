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

using FlowtideDotNet.Connector.OpenFGA.Internal.Models;
using FlowtideDotNet.Substrait.Relations;
using OpenFga.Sdk.Model;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    internal class FlowtideZanzibarConverter
    {
        private sealed class TypeReference
        {
            public TypeReference(string type, string relation)
            {
                Type = type;
                Relation = relation;
            }

            public string Type { get; }

            public string Relation { get; }

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

        private sealed class Result
        {
            public required ZanzibarRelation Relation { get; set; }
            public required HashSet<ResultUserType> ResultTypes { get; set; }
        }

        private readonly AuthorizationModel authorizationModel;
        private readonly HashSet<string> _stopTypes;
        private readonly HashSet<TypeReference> visitedTypes;
        private readonly HashSet<TypeReference> loopFoundTypes;
        private readonly List<ZanzibarRelation> _relations;

        public FlowtideZanzibarConverter(AuthorizationModel authorizationModel, HashSet<string> stopTypes)
        {
            this.authorizationModel = authorizationModel;
            _stopTypes = stopTypes;
            visitedTypes = new HashSet<TypeReference>();
            loopFoundTypes = new HashSet<TypeReference>();
            _relations = new List<ZanzibarRelation>();
        }

        public List<ZanzibarRelation> Parse(string type, string relation)
        {
            var typeDefinition = authorizationModel.TypeDefinitions.Find(x => x.Type.Equals(type, StringComparison.OrdinalIgnoreCase));

            if (typeDefinition == null)
            {
                throw new InvalidOperationException($"Type {type} not found in authorization model");
            }
            if (typeDefinition.Relations == null)
            {
                throw new InvalidOperationException($"Type {type} has no relations defined");
            }
            // Find the relation definition
            if (!typeDefinition.Relations.TryGetValue(relation, out var relationDefinition))
            {
                throw new InvalidOperationException($"Relation {relation} not found in type {type}");
            }

            var result = VisitRelationDefinition(relationDefinition, relation, typeDefinition);
            _relations.Add(result.Relation);
            return _relations;
        }

        private Result VisitRelationDefinition(Userset relationDefinition, string relationName, TypeDefinition typeDefinition)
        {
            Result? result = default;
            if (relationDefinition.This != null)
            {
                result = VisitThis(relationName, typeDefinition);
            }
            if (relationDefinition.Union != null)
            {
                result = VisitUnion(relationDefinition.Union, relationName, typeDefinition);
            }
            if (relationDefinition.ComputedUserset != null)
            {
                result = VisitComputedUserset(relationDefinition.ComputedUserset, relationName, typeDefinition);
            }
            if (relationDefinition.TupleToUserset != null)
            {
                result = VisitTupleToUserset(relationDefinition.TupleToUserset, relationName, typeDefinition);
            }
            if (relationDefinition.Intersection != null)
            {
                result = VisitIntersection(relationDefinition.Intersection, relationName, typeDefinition);
            }

            if (result != null)
            {
                return result;
            }
            throw new NotImplementedException();
        }

        private Result VisitIntersection(Usersets userset, string relationName, TypeDefinition typeDefinition)
        {
            var first = userset.Child[0];
            var firstRel = VisitRelationDefinition(first, relationName, typeDefinition);

            var rootRel = firstRel.Relation;
            var resultTypes = firstRel.ResultTypes.ToHashSet();

            for (int i = 1; i < userset.Child.Count; i++)
            {
                var subRel = VisitRelationDefinition(userset.Child[i], relationName, typeDefinition);

                var combinedTypes = new HashSet<ResultUserType>(resultTypes);
                foreach (var t in subRel.ResultTypes)
                {
                    combinedTypes.Add(t);
                }

                bool containsWildcard = combinedTypes.Any(x => x.Wildcard);
                if (!containsWildcard)
                {
                    // INNER JOIN ON l.user_type = r.user_type AND l.user_id = r.user_id
                    rootRel = new ZanzibarJoinOnUserTypeId()
                    {
                        Left = rootRel,
                        Right = subRel.Relation
                    };
                }
                else
                {
                    var rootRelReference = new ZanzibarRelationReference()
                    {
                        ReferenceId = _relations.Count
                    };
                    _relations.Add(rootRel);
                    var subRelReference = new ZanzibarRelationReference()
                    {
                        ReferenceId = _relations.Count
                    };
                    _relations.Add(subRel.Relation);
                    List<ZanzibarRelation> relations = new List<ZanzibarRelation>();
                    var joinEqual = new ZanzibarJoinOnUserTypeId()
                    {
                        Left = rootRelReference,
                        Right = subRelReference
                    };

                    relations.Add(joinEqual);
                    var leftHasWildcard = resultTypes.Any(x => x.Wildcard);
                    var rightHasWildcard = subRel.ResultTypes.Any(x => x.Wildcard);

                    if (leftHasWildcard)
                    {
                        relations.Add(new ZanzibarJoinIntersectWildcard()
                        {
                            Left = rootRelReference,
                            Right = subRelReference,
                            LeftWildcard = true
                        });
                    }

                    if (rightHasWildcard)
                    {
                        relations.Add(new ZanzibarJoinIntersectWildcard()
                        {
                            Left = rootRelReference,
                            Right = subRelReference,
                            LeftWildcard = false
                        });
                    }

                    resultTypes = combinedTypes;
                    rootRel = new ZanzibarUnion()
                    {
                        Inputs = relations
                    };
                }
            }

            return new Result()
            {
                Relation = rootRel,
                ResultTypes = resultTypes
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

        private Result VisitTupleToUserset(TupleToUserset tupleToUserset, string toRelationName, TypeDefinition typeDefinition)
        {
            var tuplesetResult = VisitTupleset(tupleToUserset.Tupleset, typeDefinition);
            if (tuplesetResult.ResultTypes.Count > 1)
            {
                throw new InvalidOperationException("At this time only 1 type is allowed for tuple to userset");
            }
            var resultType = tuplesetResult.ResultTypes.First();

            var resultTypeDefinition = authorizationModel.TypeDefinitions.Find(x => x.Type.Equals(resultType.TypeName, StringComparison.OrdinalIgnoreCase));

            if (resultTypeDefinition == null)
            {
                throw new InvalidOperationException($"Type {resultType.TypeName} not found in authorization model");
            }

            if (_stopTypes.Contains(resultTypeDefinition.Type))
            {
                var changeNameRel = new ZanzibarChangeRelationName()
                {
                    Input = tuplesetResult.Relation,
                    NewRelationName = toRelationName
                };
                return new Result()
                {
                    Relation = changeNameRel,
                    ResultTypes = tuplesetResult.ResultTypes
                };
            }

            var computedTypeResult = VisitComputedUserset(tupleToUserset.ComputedUserset, toRelationName, resultTypeDefinition);

            var joinRel = new ZanzibarJoinUserToObject()
            {
                Left = tuplesetResult.Relation,
                Right = computedTypeResult.Relation
            };

            var changeRelationName = new ZanzibarChangeRelationName()
            {
                Input = joinRel,
                NewRelationName = toRelationName
            };

            return new Result()
            {
                Relation = changeRelationName,
                ResultTypes = computedTypeResult.ResultTypes
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

            var changeNameProjection = new ZanzibarChangeRelationName()
            {
                Input = relation.Relation,
                NewRelationName = toRelationName
            };

            return new Result()
            {
                Relation = changeNameProjection,
                ResultTypes = relation.ResultTypes
            };
        }

        private Result VisitUnion(Usersets usersets, string relationName, TypeDefinition typeDefinition)
        {
            var typeRef = new TypeReference(typeDefinition.Type, relationName);
            if (visitedTypes.Contains(typeRef))
            {
                loopFoundTypes.Add(typeRef);
                return new Result()
                {
                    Relation = new ZanzibarReadLoop()
                    {
                        Relation = relationName,
                        Type = typeDefinition.Type
                    },
                    ResultTypes = new HashSet<ResultUserType>()
                };
            }
            // Add to visited types
            visitedTypes.Add(typeRef);

            List<ZanzibarRelation> relations = new List<ZanzibarRelation>();
            HashSet<ResultUserType> resultTypes = new HashSet<ResultUserType>();
            foreach (var child in usersets.Child)
            {
                var subRel = VisitRelationDefinition(child, relationName, typeDefinition);
                relations.Add(subRel.Relation);
                foreach (var t in subRel.ResultTypes)
                {
                    resultTypes.Add(t);
                }
            }

            ZanzibarRelation rel = new ZanzibarUnion()
            {
                Inputs = relations
            };
            if (loopFoundTypes.Contains(typeRef))
            {
                loopFoundTypes.Remove(typeRef);
                rel = new ZanzibarLoop()
                {
                    LoopRelation = rel,
                    Type = typeDefinition.Type,
                    Relation = relationName
                };
            }

            // Remove from visited types
            visitedTypes.Remove(typeRef);

            return new Result()
            {
                Relation = rel,
                ResultTypes = resultTypes
            };
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

            HashSet<ResultUserType> resultTypes = new HashSet<ResultUserType>();
            List<ZanzibarRelation> relations = new List<ZanzibarRelation>();
            foreach (var t in thisRelation.DirectlyRelatedUserTypes)
            {
                var typeDef = authorizationModel.TypeDefinitions.Find(x => x.Type == t.Type);
                if (typeDef == null)
                {
                    throw new InvalidOperationException();
                }

                if (t.Relation != null)
                {
                    var rel = VisitUserRelation(t, relationName, typeDef, typeDefinition);
                    relations.Add(rel.Relation);
                    foreach(var resultType in rel.ResultTypes)
                    {
                        resultTypes.Add(resultType);
                    }
                }
                else
                {
                    var typeResult = VisitDirectUser(t, typeDef, relationName, typeDefinition);
                    relations.Add(typeResult.Relation);
                    foreach(var resultType in typeResult.ResultTypes)
                    {
                        resultTypes.Add(resultType);
                    }
                }
            }

            if (relations.Count > 1)
            {
                return new Result()
                {
                    ResultTypes = resultTypes,
                    Relation = new ZanzibarUnion()
                    {
                        Inputs = relations
                    }
                };
            }
            else
            {
                return new Result()
                {
                    Relation = relations[0],
                    ResultTypes = resultTypes
                };
            }
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

            var readRelation = new ZanzibarReadUserRelation()
            {
                ObjectType = objectType.Type,
                Relation = toRelationName,
                UserRelation = relationReference.Relation,
                UserType = referenceType.Type
            };

            var result = VisitRelationDefinition(relationDef, relationReference.Relation, referenceType);

            var joinRel = new ZanzibarJoinUserRelation()
            {
                Left = readRelation,
                Right = result.Relation
            };

            return new Result()
            {
                Relation = joinRel,
                ResultTypes = result.ResultTypes
            };
        }

        private static Result VisitDirectUser(RelationReference relationReference, TypeDefinition referenceType, string relationName, TypeDefinition objectType)
        {
            var rel = new ZanzibarReadUserAndObjectType()
            {
                ObjectType = objectType.Type,
                Relation = relationName,
                UserType = referenceType.Type
            };

            return new Result()
            {
                Relation = rel,
                ResultTypes = new HashSet<ResultUserType>()
                {
                    new ResultUserType()
                    {
                        TypeName = relationReference.Type,
                        Wildcard = relationReference.Wildcard != null
                    }
                }
            };
        }
    }
}
