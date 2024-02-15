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

using FlowtideDotNet.Zanzibar.QueryPlanner.Models;
using System.Reflection.Metadata;

namespace FlowtideDotNet.Zanzibar.QueryPlanner
{
    internal sealed class ResultUserType
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
    internal sealed class Result
    {
        public required ZanzibarRelation Relation { get; set; }
        public required HashSet<ResultUserType> ResultTypes { get; set; }
    }


    internal record ConvertState(string toRelationName, ZanzibarType typeDefinition);

    internal class ZanzibarFlowtideConvertVisitor : ZanzibarTypeRelationVisitor<Result, ConvertState>
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

        private readonly ZanzibarSchema _schema;
        private readonly HashSet<string> _stopTypes;
        private readonly HashSet<TypeReference> visitedTypes;
        private readonly HashSet<TypeReference> loopFoundTypes;
        private readonly List<ZanzibarRelation> _relations;

        public ZanzibarFlowtideConvertVisitor(ZanzibarSchema schema, HashSet<string> stopTypes)
        {
            this._schema = schema;
            this._stopTypes = stopTypes;
            visitedTypes = new HashSet<TypeReference>();
            loopFoundTypes = new HashSet<TypeReference>();
            _relations = new List<ZanzibarRelation>();
        }

        public List<ZanzibarRelation> Parse(string type, string relation)
        {
            if (!_schema.Types.TryGetValue(type, out var typeDefinition))
            {
                throw new InvalidOperationException($"Type {type} not found in the zanzibar schema.");
            }
            if (!typeDefinition.Relations.TryGetValue(relation, out var relationDefinition))
            {
                throw new InvalidOperationException($"Relation {relation} not found in type {type}");
            }
            var result = Visit(relationDefinition, new ConvertState(relation, typeDefinition));
            _relations.Add(result.Relation);
            return _relations;
        }

        public override Result VisitComputedUserset(ZanzibarComputedUsersetRelation relation, ConvertState state)
        {
            if (!state.typeDefinition.Relations.TryGetValue(relation.ReferenceRelation, out var relationDef))
            {
                throw new InvalidOperationException($"Relation {relation.ReferenceRelation} not found in type {state.typeDefinition.Name}");
            }
            var result = Visit(relationDef, new ConvertState(relation.ReferenceRelation, state.typeDefinition));
            var changeNameProjection = new ZanzibarChangeRelationName()
            {
                Input = result.Relation,
                NewRelationName = state.toRelationName
            };

            return new Result()
            {
                Relation = changeNameProjection,
                ResultTypes = result.ResultTypes
            };
        }

        public override Result VisitIntersect(ZanzibarIntersectRelation relation, ConvertState state)
        {
            var first = relation.Children[0];
            var firstRel = Visit(first, state);

            var rootRel = firstRel.Relation;
            var resultTypes = firstRel.ResultTypes.ToHashSet();
            for (int i = 1; i < relation.Children.Count; i++)
            {
                var subRel = Visit(relation.Children[i], state);

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

        public override Result VisitThis(ZanzibarThisRelation relation, ConvertState state)
        {
            HashSet<ResultUserType> resultTypes = new HashSet<ResultUserType>();
            List<ZanzibarRelation> relations = new List<ZanzibarRelation>();
            foreach(var type in relation.Types)
            {
                if (!_schema.Types.TryGetValue(type.Name, out var typeDefinition))
                {
                    throw new InvalidOperationException();
                }
                if (type.Relation != null)
                {
                    var rel = VisitUserRelation(type, state.toRelationName, typeDefinition, state.typeDefinition);
                    relations.Add(rel.Relation);
                    foreach (var resultType in rel.ResultTypes)
                    {
                        resultTypes.Add(resultType);
                    }
                }
                else
                {
                    var typeResult = VisitDirectUser(type, typeDefinition, state.toRelationName, state.typeDefinition);
                    relations.Add(typeResult.Relation);
                    foreach (var resultType in typeResult.ResultTypes)
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

        public override Result VisitTupleToUserset(ZanzibarTupleToUsersetRelation relation, ConvertState state)
        {
            var tuplesetResult = VisitTupleset(relation.ReferenceRelation, state.typeDefinition);

            if (tuplesetResult.ResultTypes.Count > 1)
            {
                throw new InvalidOperationException("At this time only 1 type is allowed for tuple to userset");
            }
            var resultType = tuplesetResult.ResultTypes.First();
            
            if (!_schema.Types.TryGetValue(resultType.TypeName, out var resultTypeDefinition))
            {
                throw new InvalidOperationException($"Type {resultType.TypeName} not found in the schema");
            }
            if (!resultTypeDefinition.Relations.TryGetValue(relation.PointerRelation, out var referenceRelation))
            {
                throw new InvalidOperationException();
            }

            if (_stopTypes.Contains(resultTypeDefinition.Name))
            {
                var changeNameRel = new ZanzibarChangeRelationName()
                {
                    Input = tuplesetResult.Relation,
                    NewRelationName = state.toRelationName
                };
                return new Result()
                {
                    Relation = changeNameRel,
                    ResultTypes = tuplesetResult.ResultTypes
                };
            }

            var computedTypeResult = VisitComputedUserset(new ZanzibarComputedUsersetRelation(relation.PointerRelation), new ConvertState(state.toRelationName, resultTypeDefinition));

            var joinRel = new ZanzibarJoinUserToObject()
            {
                Left = tuplesetResult.Relation,
                Right = computedTypeResult.Relation
            };

            var changeRelationName = new ZanzibarChangeRelationName()
            {
                Input = joinRel,
                NewRelationName = state.toRelationName
            };

            return new Result()
            {
                Relation = changeRelationName,
                ResultTypes = computedTypeResult.ResultTypes
            };
        }

        private Result VisitTupleset(string referenceRelation, ZanzibarType typeDefinition)
        {
            if (!typeDefinition.Relations.TryGetValue(referenceRelation, out var relationDef))
            {
                throw new InvalidOperationException($"Relation {referenceRelation} not found in type {typeDefinition.Name}");
            }
            return Visit(relationDef, new ConvertState(referenceRelation, typeDefinition));
        }

        public override Result VisitUnion(ZanzibarUnionRelation relation, ConvertState state)
        {
            var typeRef = new TypeReference(state.typeDefinition.Name, state.toRelationName);
            if (visitedTypes.Contains(typeRef))
            {
                loopFoundTypes.Add(typeRef);
                return new Result()
                {
                    Relation = new ZanzibarReadLoop()
                    {
                        Relation = state.toRelationName,
                        Type = state.typeDefinition.Name
                    },
                    ResultTypes = new HashSet<ResultUserType>()
                };
            }
            // Add to visited types
            visitedTypes.Add(typeRef);

            List<ZanzibarRelation> relations = new List<ZanzibarRelation>();
            HashSet<ResultUserType> resultTypes = new HashSet<ResultUserType>();
            foreach (var child in relation.Children)
            {
                var subRel = Visit(child, new ConvertState(state.toRelationName, state.typeDefinition));
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
                    Type = state.typeDefinition.Name,
                    Relation = state.toRelationName
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

        private Result VisitUserRelation(ZanzibarTypeReference relationReference, string toRelationName, ZanzibarType referenceType, ZanzibarType objectType)
        {
            if (relationReference.Relation == null)
            {
                throw new InvalidOperationException();
            }
            if (!referenceType.Relations.TryGetValue(relationReference.Relation, out var relationDef))
            {
                throw new InvalidOperationException($"Relation {relationReference.Relation} not found in type {referenceType.Name}");
            }
            var readRelation = new ZanzibarReadUserRelation()
            {
                ObjectType = objectType.Name,
                Relation = toRelationName,
                UserRelation = relationReference.Relation,
                UserType = referenceType.Name
            };

            var result = Visit(relationDef, new ConvertState(relationReference.Relation, referenceType));

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

        private static Result VisitDirectUser(ZanzibarTypeReference relationReference, ZanzibarType referenceType, string relationName, ZanzibarType objectType)
        {
            var rel = new ZanzibarReadUserAndObjectType()
            {
                ObjectType = objectType.Name,
                Relation = relationName,
                UserType = referenceType.Name
            };

            return new Result()
            {
                Relation = rel,
                ResultTypes = new HashSet<ResultUserType>()
                {
                    new ResultUserType()
                    {
                        TypeName = relationReference.Name,
                        Wildcard = relationReference.Wildcard
                    }
                }
            };
        }
    }
}
