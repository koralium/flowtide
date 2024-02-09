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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    public class FlowtideOpenFgaParser
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

        private readonly AuthorizationModel authorizationModel;
        private readonly HashSet<TypeReference> foundReferences;
        private readonly HashSet<TypeReference> handledReferences;

        public FlowtideOpenFgaParser(AuthorizationModel authorizationModel)
        {
            this.authorizationModel = authorizationModel;
            foundReferences = new HashSet<TypeReference>();
            handledReferences = new HashSet<TypeReference>();
        }

        public void Parse(string type, string relation)
        {
            foundReferences.Clear();
            handledReferences.Clear();
            foundReferences.Add(new TypeReference(type, relation));
            Start();
        }

        private (TypeDefinition typeDefinition, Userset relationDefinition) GetTypeAndRelation(TypeReference typeReference)
        {
            var typeDefinition = authorizationModel.TypeDefinitions.Find(x => x.Type.Equals(typeReference.Type, StringComparison.OrdinalIgnoreCase));

            if (typeDefinition == null)
            {
                throw new InvalidOperationException($"Type {typeReference.Type} not found in authorization model");
            }
            if (typeDefinition.Relations == null)
            {
                throw new InvalidOperationException($"Type {typeReference.Type} has no relations defined");
            }
            // Find the relation definition
            if (!typeDefinition.Relations.TryGetValue(typeReference.Relation, out var relationDefinition))
            {
                throw new InvalidOperationException($"Relation {typeReference.Relation} not found in type {typeReference.Type}");
            }

            return (typeDefinition, relationDefinition);
        }

        private void Start()
        {
            List<ZanzibarRelation> relations = new List<ZanzibarRelation>();
            while (handledReferences.Count < foundReferences.Count)
            {
                // Take out the first unhandled reference
                var t = foundReferences.Except(handledReferences).First();
                handledReferences.Add(t);

                // Find the type definition
                var (typeDefinition, relationDefinition) = GetTypeAndRelation(t);

                var resolvedRelation = VisitRelationDefinition(relationDefinition, t.Relation, typeDefinition);
                relations.Add(resolvedRelation);
            }
        }

        private ZanzibarRelation VisitRelationDefinition(Userset userset, string relationName, TypeDefinition typeDefinition)
        {
            if (userset.This != null)
            {
                return VisitThis(relationName, typeDefinition);
            }
            if (userset.Union != null)
            {
                return VisitUnion(userset.Union, relationName, typeDefinition);
            }
            if (userset.ComputedUserset != null)
            {
                return VisitComputedUserset(userset.ComputedUserset, relationName, typeDefinition);
            }
            throw new NotImplementedException();
        }
        private ZanzibarRelation VisitComputedUserset(ObjectRelation objectRelation, string toRelationName, TypeDefinition typeDefinition)
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

            return new ZanzibarChangeRelationName()
            {
                Input = relation,
                NewRelationName = toRelationName
            };
        }

        private ZanzibarRelation VisitUnion(Usersets usersets, string relationName, TypeDefinition typeDefinition)
        {
            Union union = new Union()
            {
                Inputs = new List<ZanzibarRelation>()
            };

            foreach(var child in usersets.Child)
            {
                var subRel = VisitRelationDefinition(child, relationName, typeDefinition);
                union.Inputs.Add(subRel);
            }
            return union;
        }

        private ZanzibarRelation VisitThis(string relationName, TypeDefinition typeDefinition)
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
                    relations.Add(rel);
                }
                else
                {
                    var typeResult = VisitDirectUser(typeDef, relationName, typeDefinition);
                    relations.Add(typeResult);
                }
            }

            if (relations.Count > 1)
            {
                return new Union()
                {
                    Inputs = relations
                };
            }
            else
            {
                return relations[0];
            }
        }

        private ZanzibarRelation VisitUserRelation(RelationReference relationReference, string toRelationName, TypeDefinition referenceType, TypeDefinition objectType)
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

            // Add as a found relation
            foundReferences.Add(new TypeReference(referenceType.Type, relationReference.Relation));

            var readLookup = new ZanzibarReadLookup()
            {
                ObjectType = referenceType.Type,
                Relation = relationReference.Relation
            };

            var joinRel = new ZanzibarJoin()
            {
                Left = readRelation,
                Right = readLookup
            };

            return joinRel;
        }

        private ZanzibarRelation VisitDirectUser(TypeDefinition referenceType, string relationName, TypeDefinition objectType)
        {
            return new ZanzibarReadUserAndObjectType()
            {
                ObjectType = objectType.Type,
                Relation = relationName,
                UserType = referenceType.Type
            };
        }
    }
}
