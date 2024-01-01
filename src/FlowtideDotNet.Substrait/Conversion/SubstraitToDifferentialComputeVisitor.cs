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
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait.Conversion
{
    internal class SubstraitToDifferentialComputeVisitor : RelationVisitor<Relation, object>
    {
        private readonly bool addWrite;
        private readonly string tableName;
        private readonly List<string> primaryKeys;

        public SubstraitToDifferentialComputeVisitor(bool addWrite, string tableName, List<string> primaryKeys)
        {
            this.addWrite = addWrite;
            this.tableName = tableName;
            this.primaryKeys = primaryKeys;
        }
        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object state)
        {
            projectRelation.Input = Visit(projectRelation.Input, state);
            return projectRelation;
        }

        public override Relation VisitRootRelation(RootRelation rootRelation, object state)
        {
            var input = Visit(rootRelation.Input, state);
            var output = input;
            if (addWrite)
            {
                var types = new List<Type.SubstraitBaseType>();
                foreach(var name in rootRelation.Names)
                {
                    bool nullable = true;
                    if (primaryKeys.Contains(name, StringComparer.InvariantCultureIgnoreCase))
                    {
                        nullable = false;
                    }
                    types.Add(new AnyType()
                    {
                        Nullable = nullable
                    });
                }
                output = new WriteRelation()
                {
                    Input = input,
                    TableSchema = new Type.NamedStruct()
                    {
                        Names = rootRelation.Names,
                        Struct = new Type.Struct()
                        {
                            Types = types
                        }
                    },
                    NamedObject = new NamedTable()
                    {
                        Names = new List<string>() { tableName }
                    }
                };
            }
            rootRelation.Input = output;
            return rootRelation;
        }

        public override Relation VisitWriteRelation(WriteRelation writeRelation, object state)
        {
            writeRelation.Input = Visit(writeRelation.Input, state);
            return writeRelation;
        }

        public override Relation VisitUnwrapRelation(UnwrapRelation unwrapRelation, object state)
        {
            unwrapRelation.Input = Visit(unwrapRelation.Input, state);
            return unwrapRelation;
        }

        public override Relation VisitSetRelation(SetRelation setRelation, object state)
        {
            for(int i = 0; i < setRelation.Inputs.Count; i++)
            {
                setRelation.Inputs[i] = Visit(setRelation.Inputs[i], state);
            }
            return setRelation;
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            if (filterRelation.Input is ReadRelation readRelation)
            {
                readRelation.Filter = filterRelation.Condition;
                return Visit(filterRelation.Input, state);
            }
            return base.VisitFilterRelation(filterRelation, state);
        }

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            int keyIndex = -1;
            if (readRelation.BaseSchema.Struct == null)
            {
                throw new NotSupportedException("Struct must be defined with types");
            }
            for (int i = 0; i < readRelation.BaseSchema.Struct.Types.Count; i++)
            {
                var type = readRelation.BaseSchema.Struct.Types[i];
                if (type.Nullable == false)
                {
                    keyIndex = i;
                }
            }
            if (keyIndex == -1)
            {
                throw new NotSupportedException("One column must be not nullable");
            }
            

            var normalizationRelation = new NormalizationRelation()
            {
                Filter = readRelation.Filter,
                Input = readRelation,
                KeyIndex = new List<int>() { keyIndex }
            };

            readRelation.Filter = null;

            return normalizationRelation;
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            joinRelation.Left = Visit(joinRelation.Left, state);
            joinRelation.Right = Visit(joinRelation.Right, state);


            //if (joinRelation.Expression is AndFunction andFunction)
            //{
            //    List<Expressions.Expression> postFilterExpressions = new List<Expressions.Expression>();
            //    for (int i = 0; i < andFunction.Arguments.Count; i++)
            //    {
            //        var arg = andFunction.Arguments[i];

            //        if (arg is BooleanComparison booleanComparison)
            //        {
            //            if (booleanComparison.Left is Literal || booleanComparison.Right is Literal)
            //            {
            //                postFilterExpressions.Add(booleanComparison);
            //                andFunction.Arguments.RemoveAt(i);
            //                i--;
            //            }
            //        }
            //        else
            //        {
            //            postFilterExpressions.Add(arg);
            //            andFunction.Arguments.RemoveAt(i);
            //            i--;
            //        }
            //    }

            //    if (postFilterExpressions.Count == 1)
            //    {
            //        joinRelation.PostJoinFilter = postFilterExpressions[0];
            //    }
            //    else if (postFilterExpressions.Count > 1)
            //    {
            //        joinRelation.PostJoinFilter = new AndFunction()
            //        {
            //            Arguments = postFilterExpressions
            //        };
            //    }
            //    if (andFunction.Arguments.Count == 1)
            //    {
            //        joinRelation.Expression = andFunction.Arguments[0];
            //    }
            //    else if (andFunction.Arguments.Count == 0)
            //    {
            //        joinRelation.Expression = new BoolLiteral() { Value = true };
            //    }
            //}

            return joinRelation;
        }

        public override Relation VisitNormalizationRelation(NormalizationRelation normalizationRelation, object state)
        {
            return normalizationRelation;
        }

        public override Relation VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, object state)
        {
            return base.VisitVirtualTableReadRelation(virtualTableReadRelation, state);
        }
    }
}
