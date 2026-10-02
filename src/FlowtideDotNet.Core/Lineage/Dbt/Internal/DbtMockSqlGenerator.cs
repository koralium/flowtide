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

using FlowtideDotNet.Core.Lineage.Internal;
using System.Globalization;
using System.Text;

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    internal sealed class DbtMockSql
    {
        public required string Sql { get; init; }

        // Upstream tables in alias order, t0 first.
        public required IReadOnlyList<DbtTableIdentity> FromOrder { get; init; }

        public required bool SelfReference { get; init; }
    }

    // Lineage encoding only, never a reconstruction of the query.
    internal static class DbtMockSqlGenerator
    {
        public static DbtMockSql Generate(DbtMergedTable model, Func<DbtTableIdentity, string> uniqueIdOf, DbtSqlDialectInfo dialect)
        {
            var selfReference = false;
            var aliases = new Dictionary<DbtTableIdentity, int>();
            var fromOrder = new List<DbtTableIdentity>();

            bool IsUsable(DbtInputRef inputRef)
            {
                if (inputRef.Identity is not DbtTableIdentity id)
                {
                    return false;
                }
                if (id == model.Identity)
                {
                    selfReference = true;
                    return false;
                }
                return true;
            }

            void AddAlias(DbtTableIdentity identity)
            {
                if (aliases.TryAdd(identity, fromOrder.Count))
                {
                    fromOrder.Add(identity);
                }
            }

            string Render(DbtInputRef inputRef)
            {
                return "t" + aliases[inputRef.Identity!.Value].ToString(CultureInfo.InvariantCulture) + "." + dialect.Quote(inputRef.Field);
            }

            foreach (var column in model.OutputColumns)
            {
                if (model.Fields.TryGetValue(column, out var refs))
                {
                    foreach (var inputRef in refs.Refs.Where(IsUsable))
                    {
                        AddAlias(inputRef.Identity!.Value);
                    }
                }
            }
            var datasetRefs = model.Dataset.Refs.Where(IsUsable).ToList();
            foreach (var inputRef in datasetRefs)
            {
                AddAlias(inputRef.Identity!.Value);
            }
            if (model.Reachable.Contains(model.Identity))
            {
                selfReference = true;
            }
            var remaining = model.Reachable
                .Where(x => x != model.Identity && !aliases.ContainsKey(x))
                .OrderBy(uniqueIdOf, StringComparer.Ordinal)
                .ToList();
            foreach (var identity in remaining)
            {
                AddAlias(identity);
            }

            var builder = new StringBuilder("SELECT\n");
            var projections = new List<string>(model.OutputColumns.Count);
            foreach (var column in model.OutputColumns)
            {
                var refs = model.Fields.TryGetValue(column, out var fieldRefs) ? fieldRefs.Refs.Where(IsUsable).ToList() : [];
                // Direct inputs first, then indirect, Flowtide order inside.
                var rendered = refs.Where(x => x.IsDirect)
                    .Concat(refs.Where(x => !x.IsDirect))
                    .Select(Render)
                    .Distinct(StringComparer.Ordinal)
                    .ToList();
                projections.Add("  " + Expression(rendered, "NULL") + " AS " + dialect.Quote(column));
            }
            builder.Append(projections.Count == 0 ? "  NULL" : string.Join(",\n", projections));

            var joinRefs = new List<List<string>>(fromOrder.Count);
            for (int i = 0; i < fromOrder.Count; i++)
            {
                joinRefs.Add(datasetRefs
                    .Where(x => aliases[x.Identity!.Value] == i && x.Has(LineageTransformationSubtype.Join))
                    .Select(Render)
                    .Distinct(StringComparer.Ordinal)
                    .ToList());
            }

            var landed = new HashSet<string>(StringComparer.Ordinal);
            for (int i = 0; i < fromOrder.Count; i++)
            {
                var relation = dialect.QuoteRelation(fromOrder[i]) + " AS t" + i.ToString(CultureInfo.InvariantCulture);
                if (i == 0)
                {
                    builder.Append("\nFROM ").Append(relation);
                    continue;
                }
                if (joinRefs[i].Count == 0)
                {
                    builder.Append("\nCROSS JOIN ").Append(relation);
                    continue;
                }
                var earlier = joinRefs.Take(i).SelectMany(x => x).Distinct(StringComparer.Ordinal).ToList();
                builder.Append("\nINNER JOIN ").Append(relation).Append(" ON ");
                if (earlier.Count > 0)
                {
                    builder.Append(Expression(earlier, string.Empty)).Append(" = ").Append(Expression(joinRefs[i], string.Empty));
                    landed.UnionWith(earlier);
                }
                else
                {
                    builder.Append(Expression(joinRefs[i], string.Empty)).Append(" IS NOT NULL");
                }
                landed.UnionWith(joinRefs[i]);
            }

            // Filters and unplaced join keys only feed table lineage.
            var where = new List<string>();
            foreach (var inputRef in datasetRefs)
            {
                var rendered = Render(inputRef);
                var isFilter = inputRef.Has(LineageTransformationSubtype.Filter);
                var isUnplacedJoin = inputRef.Has(LineageTransformationSubtype.Join) && !landed.Contains(rendered);
                if ((isFilter || isUnplacedJoin) && !where.Contains(rendered))
                {
                    where.Add(rendered);
                }
            }
            if (where.Count > 0)
            {
                builder.Append("\nWHERE ").Append(string.Join(" AND ", where.Select(x => x + " IS NOT NULL")));
            }

            var groupBy = datasetRefs
                .Where(x => x.Has(LineageTransformationSubtype.GroupBy))
                .Select(Render)
                .Distinct(StringComparer.Ordinal)
                .ToList();
            if (groupBy.Count > 0)
            {
                builder.Append("\nGROUP BY ").Append(string.Join(", ", groupBy));
            }

            return new DbtMockSql()
            {
                Sql = builder.ToString(),
                FromOrder = fromOrder,
                SelfReference = selfReference
            };
        }

        // COALESCE carries every input, T-SQL needs two arguments.
        private static string Expression(IReadOnlyList<string> refs, string empty)
        {
            return refs.Count switch
            {
                0 => empty,
                1 => refs[0],
                _ => "COALESCE(" + string.Join(", ", refs) + ")"
            };
        }
    }
}
