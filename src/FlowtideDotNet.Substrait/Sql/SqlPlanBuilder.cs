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
using FlowtideDotNet.Substrait.Sql.Internal;
using FlowtideDotNet.Substrait.Type;
using SqlParser;

namespace FlowtideDotNet.Substrait.Sql
{
    public class SqlPlanBuilder
    {
        internal readonly TablesMetadata _tablesMetadata = new TablesMetadata();
        private readonly Parser _parser = new Parser();
        internal PlanModifier _planModifier = new PlanModifier();
        private SqlFunctionRegister _sqlFunctionRegister;

        public SqlPlanBuilder()
        {
            _sqlFunctionRegister = new SqlFunctionRegister();
            BuiltInSqlFunctions.AddBuiltInFunctions(_sqlFunctionRegister);
        }

        public ISqlFunctionRegister FunctionRegister => _sqlFunctionRegister;

        public void AddTableDefinition(string name, NamedStruct schema)
        {
            _tablesMetadata.AddTable(name, schema);
        }

        public void AddTableProvider(ITableProvider tableProvider)
        {
            _tablesMetadata.AddTableProvider(tableProvider);
        }

        public void AddPlanAsView(string viewName, Plan plan)
        {
            List<string>? names = default;
            for(int i = 0; i < plan.Relations.Count; i++)
            {
                if (plan.Relations[i] is RootRelation rootRelation)
                {
                    names = rootRelation.Names;
                }
            }
            if (names == null)
            {
                throw new InvalidOperationException("No root relation exists");
            }
            List<SubstraitBaseType> types = new List<SubstraitBaseType>();
            for (int i = 0; i < names.Count; i++)
            {
                types.Add(new AnyType());
            }
            _planModifier.AddPlanAsView(viewName, plan);
            _tablesMetadata.AddTable(viewName, new NamedStruct()
            {
                Names = names,
                Struct = new Struct()
                {
                    Types = types
                }
            });
        }

        public void Sql(string sqlText)
        {
            
            var statements = _parser.ParseSql(sqlText, new FlowtideDialect(), new ParserOptions()
            {
                RecursionLimit = 100000
            });

            SqlSubstraitVisitor sqlSubstraitVisitor = new SqlSubstraitVisitor(this, _sqlFunctionRegister);
            foreach (var statement in statements)
            {
                var result = sqlSubstraitVisitor.Visit(statement, default);
                if (result != null)
                {
                    _planModifier.AddRootPlan(new Plan()
                    {
                        Relations = new List<FlowtideDotNet.Substrait.Relations.Relation>()
                        {
                            result.Relation
                        }
                    });
                }
            }
        }

        public Plan GetPlan()
        {
            return _planModifier.Modify();
        }
    }
}
