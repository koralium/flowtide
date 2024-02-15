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

using FlowtideDotNet.Zanzibar;
using OpenFga.Sdk.Model;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    internal static class OpenFgaToZanzibarSchema
    {
        public static ZanzibarSchema Convert(AuthorizationModel authorizationModel)
        {
            Dictionary<string, ZanzibarType> types = new Dictionary<string, ZanzibarType>();
            Dictionary<string, ZanzibarCaveat> caveats = new Dictionary<string, ZanzibarCaveat>();
            
            foreach(var type in authorizationModel.TypeDefinitions)
            {
                var convertedType = ConvertType(type);
                types.Add(convertedType.Name, convertedType);
            }

            return new ZanzibarSchema(types, caveats);
        }

        private static ZanzibarType ConvertType(TypeDefinition type)
        {
            if (type.Relations == null)
            {
                throw new InvalidOperationException($"Type {type.Type} has no relations defined");
            }
            Dictionary<string, ZanzibarTypeRelation> relations = new Dictionary<string, ZanzibarTypeRelation>();
            
            foreach(var kv in type.Relations)
            {
                relations.Add(kv.Key, ConvertRelation(kv.Value, kv.Key, type));
            }

            return new ZanzibarType(type.Type, relations);
        }

        private static ZanzibarTypeRelation ConvertRelation(Userset userset, string relationName, TypeDefinition typeDefinition)
        {
            if (userset.This != null)
            {
                return ConvertThis(relationName, typeDefinition);
            }
            if (userset.ComputedUserset != null)
            {
                return ConvertComputedUserset(userset.ComputedUserset, relationName);
            }
            if (userset.TupleToUserset != null)
            {
                return ConvertTupleToUserset(userset.TupleToUserset, relationName);
            }
            if (userset.Intersection != null)
            {
                return ConvertIntersection(userset.Intersection, relationName, typeDefinition);
            }
            if (userset.Union != null)
            {
                return ConvertUnion(userset.Union, relationName, typeDefinition);
            }
            throw new NotImplementedException();
        }

        private static ZanzibarTypeRelation ConvertUnion(Usersets union, string relationName, TypeDefinition typeDefinition)
        {
            List<ZanzibarTypeRelation> types = new List<ZanzibarTypeRelation>();
            foreach (var userType in union.Child)
            {
                types.Add(ConvertRelation(userType, relationName, typeDefinition));
            }
            return new ZanzibarUnionRelation(types);
        }

        private static ZanzibarTypeRelation ConvertIntersection(Usersets intersection, string relationName, TypeDefinition typeDefinition)
        {
            List<ZanzibarTypeRelation> types = new List<ZanzibarTypeRelation>();
            foreach (var userType in intersection.Child)
            {
                types.Add(ConvertRelation(userType, relationName, typeDefinition));
            }
            return new ZanzibarIntersectRelation(types);
        }

        private static ZanzibarTypeRelation ConvertTupleToUserset(TupleToUserset tupleToUserset, string relationName)
        {
            if (tupleToUserset.Tupleset.Relation == null)
            {
                throw new InvalidOperationException($"Relation {relationName} has no tupleset relation defined");
            }
            if (tupleToUserset.ComputedUserset.Relation == null)
            {
                throw new InvalidOperationException($"Relation {relationName} has no computed userset relation defined");
            }
            return new ZanzibarTupleToUsersetRelation(tupleToUserset.Tupleset.Relation, tupleToUserset.ComputedUserset.Relation);
        }

        private static ZanzibarTypeRelation ConvertComputedUserset(ObjectRelation objectRelation, string toRelationName)
        {
            if (objectRelation.Relation == null)
            {
                throw new InvalidOperationException($"Relation {toRelationName} has no relation defined");
            }
            return new ZanzibarComputedUsersetRelation(objectRelation.Relation);
        }

        private static ZanzibarTypeRelation ConvertThis(string relationName, TypeDefinition typeDefinition)
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
            List<ZanzibarTypeReference> types = new List<ZanzibarTypeReference>();
            foreach (var userType in thisRelation.DirectlyRelatedUserTypes)
            {
                types.Add(new ZanzibarTypeReference(userType.Type, userType.Relation, userType.Wildcard != null, userType.Condition));
            }

            return new ZanzibarThisRelation(types);
        }
    }
}
