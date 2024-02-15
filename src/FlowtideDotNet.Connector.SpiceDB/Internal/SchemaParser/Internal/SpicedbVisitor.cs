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
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.SpiceDB.Internal.SchemaParser.Internal
{
    internal class SpicedbVisitor : SpicedbParserBaseVisitor<object>
    {
        private class RelationContainer
        {
            public string Name { get; }
            public ZanzibarTypeRelation Relation { get; }

            public RelationContainer(string name, ZanzibarTypeRelation relation)
            {
                Name = name;
                Relation = relation;
            }
        }

        public override object VisitParse([NotNull] SpicedbParser.ParseContext context)
        {
            var blocks = context.block();
            Dictionary<string, ZanzibarType> types = new Dictionary<string, ZanzibarType>();
            Dictionary<string, ZanzibarCaveat> caveats = new Dictionary<string, ZanzibarCaveat>();
            foreach (var block in blocks)
            {
                var parsedBlock = Visit(block);
                if (parsedBlock is ZanzibarType zanzibarType)
                {
                    types.Add(zanzibarType.Name, zanzibarType);
                }
                else if (parsedBlock is ZanzibarCaveat caveat)
                {
                    caveats[caveat.Name] = caveat;
                }
                else
                {
                    throw new InvalidOperationException("Expected ZanzibarType");
                }
            }
            return new ZanzibarSchema(types, caveats);
        }

        public override object VisitType_definition([NotNull] SpicedbParser.Type_definitionContext context)
        {
            var name = context.definition_name.Text;
            var relations = context.type_relation();

            Dictionary<string, ZanzibarTypeRelation> typeRelations = new Dictionary<string, ZanzibarTypeRelation>();

            foreach (var relation in relations)
            {
                var relationResult = Visit(relation);
                if (relationResult is RelationContainer relationDefinition)
                {
                    typeRelations[relationDefinition.Name] = relationDefinition.Relation;
                }
                else
                {
                    throw new InvalidOperationException("Expected RelationDefinition");
                }
            }

            return new ZanzibarType(name, typeRelations);
        }

        public override object VisitType_relation([NotNull] SpicedbParser.Type_relationContext context)
        {
            if (context.relation != null)
            {
                var name = context.relation_name.Text;
                var typeNames = context.relation_type();

                List<ZanzibarTypeReference> directTypes = new List<ZanzibarTypeReference>();
                foreach (var typeName in typeNames)
                {
                    var parsedTypeName = Visit(typeName);

                    if (parsedTypeName is ZanzibarTypeReference typeReference)
                    {
                        directTypes.Add(typeReference);
                    }
                    else
                    {
                        throw new InvalidOperationException("Expected string as a type name");
                    }
                }

                return new RelationContainer(name, new ZanzibarThisRelation(directTypes));
            }
            if (context.permission != null)
            {
                var permissionName = context.permission_name.Text;
                var result = Visit(context.permission_userset);
                if (result is ZanzibarTypeRelation zanzibarTypeRelation)
                {
                    return new RelationContainer(permissionName, zanzibarTypeRelation);
                }
                else
                {
                    throw new InvalidOperationException("Expected ZanzibarTypeRelation");
                }
            }
            return base.VisitType_relation(context);
        }

        public override object VisitRelation_type([NotNull] SpicedbParser.Relation_typeContext context)
        {
            string? relation = default;
            if (context.relation != null)
            {
                relation = context.relation.Text;
            }
            bool wildcard = context.star != null;
            var caveatName = context.caveat_name?.Text;
            return new ZanzibarTypeReference(context.name.Text, relation, wildcard, caveatName);
        }

        public override object VisitUserset([NotNull] SpicedbParser.UsersetContext context)
        {
            if (context.union != null)
            {
                var usersets = context.userset();
                List<ZanzibarTypeRelation> relations = new List<ZanzibarTypeRelation>();
                foreach (var userset in usersets)
                {
                    var result = Visit(userset);
                    if (result is ZanzibarTypeRelation zanzibarTypeRelation)
                    {
                        relations.Add(zanzibarTypeRelation);
                    }
                    else
                    {
                        throw new InvalidOperationException("Expected ZanzibarTypeRelation");
                    }
                }
                return new ZanzibarUnionRelation(relations);
            }
            if (context.tuple != null)
            {
                var selfRelation = context.self_relation.Text;
                var pointerRelation = context.object_relation.Text;
                return new ZanzibarTupleToUsersetRelation(selfRelation, pointerRelation);
            }
            if (context.compute != null)
            {
                var relationName = context.compute.Text;
                return new ZanzibarComputedUsersetRelation(relationName);
            }
            if (context.intersect != null)
            {
                var usersets = context.userset();
                List<ZanzibarTypeRelation> relations = new List<ZanzibarTypeRelation>();
                foreach (var userset in usersets)
                {
                    var result = Visit(userset);
                    if (result is ZanzibarTypeRelation zanzibarTypeRelation)
                    {
                        relations.Add(zanzibarTypeRelation);
                    }
                    else
                    {
                        throw new InvalidOperationException("Expected ZanzibarTypeRelation");
                    }
                }
                return new ZanzibarIntersectRelation(relations);
            }
            throw new InvalidOperationException("Expected union, tuple, compute or intersect");
        }

        public override object VisitCaveat([NotNull] SpicedbParser.CaveatContext context)
        {
            var caveatName = context.definition_name.Text;
            return new ZanzibarCaveat(caveatName);
        }
    }
}
