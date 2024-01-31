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

using FlowtideDotNet.Substrait.Relations;
using OpenFga.Sdk.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.FetchDirection;

namespace FlowtideDotNet.Connector.OpenFGA.Internal
{
    public class ModelParser
    {
        private readonly AuthorizationModel authorizationModel;
        private List<RelationshipEdge> EdgeStack = new List<RelationshipEdge>();

        private class RelationshipEdge
        {
            public required string Type { get; set; }

            public required string Relation { get; set; }
        }

        private class Result
        {
            public required List<TypeDefinition> ResultTypes { get; set; }
        }

        public ModelParser(AuthorizationModel authorizationModel)
        {
            this.authorizationModel = authorizationModel;
        }

        public void Parse(AuthorizationModel authorizationModel, string type, string relation)
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
            if (!typeDefinition.Relations.TryGetValue(relation, out var relationDefinition))
            {
                throw new InvalidOperationException($"Relation {relation} not found in type {type}");
            }
            VisitRelationDefinition(relationDefinition, relation, typeDefinition);
            //if (relationDefinition.This != null)
            //{
            //    if (!typeDefinition.Metadata.Relations.TryGetValue(relation, out var thisRelation))
            //    {
            //        throw new InvalidOperationException($"Relation {relation} not found in type {type}");
            //    }
            //}
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
                return VisitComputedUserset(relationDefinition.ComputedUserset, typeDefinition);
            }
            if (relationDefinition.TupleToUserset != null)
            {
                return VisitTupleToUserset(relationDefinition.TupleToUserset, typeDefinition);
            }
            throw new NotImplementedException();
        }

        private Result VisitTupleToUserset(TupleToUserset tupleToUserset, TypeDefinition typeDefinition)
        {
            var tuplesetResult = VisitComputedUserset(tupleToUserset.Tupleset, typeDefinition);
            VisitComputedUserset(tupleToUserset.ComputedUserset, tuplesetResult.ResultTypes[0]);
            return tuplesetResult;
        }

        private Result VisitThis(Userset userset, string relationName, TypeDefinition typeDefinition)
        {
            if (!typeDefinition.Metadata.Relations.TryGetValue(relationName, out var thisRelation))
            {
                throw new InvalidOperationException($"Relation {relationName} not found in type {typeDefinition.Type}");
            }
            var query = "SELECT user, relation, object FROM openfga WHERE object_type = '" + typeDefinition.Type + "' AND relation = '" + relationName + "'";

            List<TypeDefinition> types = new List<TypeDefinition>();

            foreach(var t in thisRelation.DirectlyRelatedUserTypes)
            {
                var typeDef = authorizationModel.TypeDefinitions.Find(x => x.Type == t.Type);
                if (typeDef == null)
                {
                    throw new InvalidOperationException();
                }
                types.Add(typeDef);
            }
            return new Result() { ResultTypes = types };
        }

        private Result VisitComputedUserset(ObjectRelation objectRelation, TypeDefinition typeDefinition)
        {
            if (!typeDefinition.Relations.TryGetValue(objectRelation.Relation, out var relationDef))
            {
                throw new InvalidOperationException($"Relation {objectRelation.Relation} not found in type {typeDefinition.Type}");
            }
            return VisitRelationDefinition(relationDef, objectRelation.Relation, typeDefinition);
            // Select all tuples that match the relation and object type
            //
        }

        private Result VisitUnion(Usersets usersets, string relationName, TypeDefinition typeDefinition)
        {
            foreach(var child in usersets.Child)
            {
                VisitRelationDefinition(child, relationName, typeDefinition);
            }
            return new Result() { ResultTypes = new List<TypeDefinition>() { typeDefinition } };
        }
    }
}
