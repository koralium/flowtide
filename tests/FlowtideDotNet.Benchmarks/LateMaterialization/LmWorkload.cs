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

using System.Text;

namespace FlowtideDotNet.Benchmarks.LateMaterialization
{
    /// <summary>
    /// A concat of output columns, or a struct of them.
    /// </summary>
    internal sealed class LmComputed
    {
        public LmComputed(bool isStruct, string[] parts)
        {
            IsStruct = isStruct;
            Parts = parts;
        }

        public bool IsStruct { get; }

        public string[] Parts { get; }
    }

    /// <summary>
    /// One reference workload, in a wide and a narrow form.
    /// </summary>
    internal sealed class LmWorkload
    {
        public static readonly string[] Names = ["LM-J5", "LM-J7", "LM-G", "LM-GM", "LM-FD", "LM-P", "LM-X", "LM-N"];

        private LmWorkload(string name, List<LmTable> tables)
        {
            Name = name;
            Tables = tables;
        }

        public string Name { get; }

        public IReadOnlyList<LmTable> Tables { get; }

        public string WideSql { get; private set; } = string.Empty;

        public string NarrowSql { get; private set; } = string.Empty;

        /// <summary>
        /// The narrow query with one reference per table.
        /// </summary>
        public string HostedSql { get; private set; } = string.Empty;

        /// <summary>
        /// The name the reference column has in the hosted query's tables.
        /// </summary>
        public const string ReferenceColumn = "rr";

        public int WideKeyColumns { get; private set; } = 1;

        public int NarrowKeyColumns { get; private set; } = 1;

        public int Parallelization { get; private set; } = 1;

        /// <summary>
        /// How many operators of each display name both plans must have.
        /// </summary>
        public Dictionary<string, int> Shape { get; } = new Dictionary<string, int>();

        /// <summary>
        /// The state classes whose wide data would stay, as the heap or as an inline key.
        /// </summary>
        public string[] KeptClasses { get; private set; } = [LmCensus.Source];

        /// <summary>
        /// A class where one tree's wide data stays.
        /// </summary>
        public string? KeptOnceClass { get; private set; }

        /// <summary>
        /// Output columns built from other output columns, by name.
        /// </summary>
        public Dictionary<string, LmComputed> Computed { get; } = new Dictionary<string, LmComputed>();

        /// <summary>
        /// The fact column that gives the group of a row, none for one row per fact.
        /// </summary>
        public string? GroupColumn { get; private set; }

        public LmTable Fact => Tables[0];

        public LmTable GetTable(string name)
        {
            for (int i = 0; i < Tables.Count; i++)
            {
                if (Tables[i].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return Tables[i];
                }
            }
            throw new InvalidOperationException($"Table {name} is not part of workload {Name}");
        }

        public long InputRows
        {
            get
            {
                long rows = 0;
                for (int i = 0; i < Tables.Count; i++)
                {
                    rows += Tables[i].Rows;
                }
                return rows;
            }
        }

        /// <summary>
        /// The group a fact row belongs to at a round.
        /// </summary>
        public long GroupOf(long seed, long row, int round)
        {
            int column = Fact.GetColumnIndex(GroupColumn!);
            return LmData.LongValue(seed, Fact, column, row, round);
        }

        public static LmWorkload Create(string name, double scale)
        {
            switch (name.ToUpperInvariant())
            {
                case "LM-J5":
                    return Join("LM-J5", scale, [2_000_000, 500_000, 50_000, 5_000], false);
                case "LM-J7":
                    return Join("LM-J7", scale, [2_000_000, 500_000, 50_000, 5_000, 500_000, 50_000], false);
                case "LM-X":
                    return Join("LM-X", scale, [2_000_000, 500_000, 50_000, 5_000, 500_000, 50_000], true);
                case "LM-G":
                    return Group("LM-G", scale, false);
                case "LM-GM":
                    return Group("LM-GM", scale, true);
                case "LM-FD":
                    return FunctionalDependency(scale);
                case "LM-P":
                    return Partitioned(scale);
                case "LM-N":
                    return Nested(scale);
                default:
                    throw new ArgumentException($"Unknown workload {name}, expected one of {string.Join(", ", Names)}");
            }
        }

        private static long Scaled(long rows, double scale)
        {
            return Math.Max(20, (long)(rows * scale));
        }

        private static LmTable Dimension(string name, long rows, double scale)
        {
            var columns = new List<LmColumn>()
            {
                new LmColumn("id", LmColumnKind.Key),
                new LmColumn("a1", LmColumnKind.Number)
            };
            // About one kilobyte of payload
            for (int i = 1; i <= 8; i++)
            {
                columns.Add(new LmColumn($"p{i}", LmColumnKind.Text, 125, payload: true));
            }
            return new LmTable(name, Scaled(rows, scale), false, columns.ToArray());
        }

        private static LmTable FactTable(long rows, double scale, List<LmTable> dimensions, LmColumn? extra = null)
        {
            var columns = new List<LmColumn>() { new LmColumn("id", LmColumnKind.Key) };
            for (int i = 0; i < dimensions.Count; i++)
            {
                columns.Add(new LmColumn(dimensions[i].Name + "_id", LmColumnKind.Reference, modulus: dimensions[i].Rows));
            }
            for (int i = 1; i <= 3; i++)
            {
                columns.Add(new LmColumn($"k{i}", LmColumnKind.Number));
            }
            // Two hundred bytes of payload
            for (int i = 1; i <= 4; i++)
            {
                columns.Add(new LmColumn($"p{i}", LmColumnKind.Text, 50, payload: true));
            }
            if (extra != null)
            {
                columns.Add(extra);
            }
            return new LmTable("fact", Scaled(rows, scale), true, columns.ToArray());
        }

        private static void AppendColumns(StringBuilder select, LmTable table, string alias, string prefix, bool wide, bool hosted = false)
        {
            bool referenceWritten = false;
            for (int i = 0; i < table.Columns.Count; i++)
            {
                var column = table.Columns[i];
                if (column.Kind == LmColumnKind.Reference || (column.Kind == LmColumnKind.Key && prefix.Length > 0))
                {
                    continue;
                }
                string name = column.Name;
                if (column.Payload && hosted)
                {
                    // One reference stands where the wide columns were
                    if (referenceWritten)
                    {
                        continue;
                    }
                    referenceWritten = true;
                    name = ReferenceColumn;
                }
                else if (column.Payload && !wide)
                {
                    continue;
                }
                if (select.Length > 0)
                {
                    select.Append(", ");
                }
                select.Append(alias).Append('.').Append(name);
                if (prefix.Length > 0 || name == ReferenceColumn)
                {
                    select.Append(" AS ").Append(alias).Append('_').Append(name);
                }
            }
        }

        private static string JoinSql(LmTable fact, List<LmTable> dimensions, bool wide, bool hosted = false)
        {
            var select = new StringBuilder();
            AppendColumns(select, fact, "f", string.Empty, wide, hosted);
            var from = new StringBuilder("FROM fact f");
            for (int i = 0; i < dimensions.Count; i++)
            {
                var name = dimensions[i].Name;
                AppendColumns(select, dimensions[i], name, name + "_", wide, hosted);
                from.Append($" INNER JOIN {name} {name} ON f.{name}_id = {name}.id");
            }
            return $"INSERT INTO output SELECT {select} {from}";
        }

        private static LmWorkload Join(string name, double scale, long[] dimensionRows, bool projection)
        {
            var dimensions = new List<LmTable>();
            for (int i = 0; i < dimensionRows.Length; i++)
            {
                dimensions.Add(Dimension($"d{i + 1}", dimensionRows[i], scale));
            }
            var fact = FactTable(20_000_000, scale, dimensions);
            var tables = new List<LmTable>() { fact };
            tables.AddRange(dimensions);
            var workload = new LmWorkload(name, tables);
            if (projection)
            {
                // The narrow form only drops the computed columns
                workload.WideSql = ProjectionSql(fact, dimensions, true, workload.Computed);
                workload.NarrowSql = ProjectionSql(fact, dimensions, false, null);
                // Two join trees hold them, one copy stays
                workload.KeptOnceClass = LmCensus.JoinIntermediate;
                workload.Shape["Multi Join"] = 1;
                workload.Shape["Merge Join Bulk"] = 2;
            }
            else
            {
                workload.WideSql = JoinSql(fact, dimensions, true);
                workload.NarrowSql = JoinSql(fact, dimensions, false);
                workload.HostedSql = JoinSql(fact, dimensions, false, true);
                workload.Shape["Multi Join"] = dimensions.Count > 4 ? 2 : 1;
            }
            return workload;
        }

        private static string ProjectionSql(LmTable fact, List<LmTable> dimensions, bool computed, Dictionary<string, LmComputed>? columns)
        {
            var view = new StringBuilder();
            AppendColumns(view, fact, "f", string.Empty, true);
            AppendColumns(view, dimensions[0], "d1", "d1_", true);
            var outer = new StringBuilder();
            foreach (var part in view.ToString().Split(", "))
            {
                // The outer query reads the view's columns by their names
                int alias = part.IndexOf(" AS ", StringComparison.Ordinal);
                string column = alias >= 0 ? part.Substring(alias + 4) : part.Substring(part.IndexOf('.') + 1);
                outer.Append(outer.Length > 0 ? ", " : string.Empty).Append("v.").Append(column);
            }
            for (int i = 1; i < dimensions.Count; i++)
            {
                view.Append($", f.{dimensions[i].Name}_id");
            }
            if (computed)
            {
                // Ten computed strings of a hundred bytes
                int count = 0;
                for (int a = 1; a <= 4 && count < 10; a++)
                {
                    for (int b = 1; b <= 4 && count < 10; b++)
                    {
                        if (a == b)
                        {
                            continue;
                        }
                        count++;
                        view.Append($", concat(f.p{a}, f.p{b}) AS c{count}");
                        columns?.Add($"c{count}", new LmComputed(false, [$"p{a}", $"p{b}"]));
                        outer.Append($", v.c{count}");
                    }
                }
            }
            var from = new StringBuilder("FROM v v");
            for (int i = 1; i < dimensions.Count; i++)
            {
                var name = dimensions[i].Name;
                AppendColumns(outer, dimensions[i], name, name + "_", true);
                from.Append($" INNER JOIN {name} {name} ON v.{name}_id = {name}.id");
            }
            return $"CREATE VIEW v AS SELECT {view} FROM fact f INNER JOIN d1 d1 ON f.d1_id = d1.id;\nINSERT INTO output SELECT {outer} {from}";
        }

        private static LmWorkload Nested(double scale)
        {
            var dimensions = new List<LmTable>() { Dimension("d1", 500_000, scale), Dimension("d2", 50_000, scale) };
            var fact = FactTable(2_000_000, scale, dimensions, new LmColumn("items", LmColumnKind.Items, 2000, payload: true));
            var tables = new List<LmTable>() { fact };
            tables.AddRange(dimensions);
            string from = "FROM fact f INNER JOIN d1 d1 ON f.d1_id = d1.id INNER JOIN d2 d2 ON f.d2_id = d2.id";
            var workload = new LmWorkload("LM-N", tables)
            {
                WideSql = $"INSERT INTO output SELECT f.id, f.k1, d1.a1 AS d1_a1, d2.a1 AS d2_a1, f.items, named_struct('a', d1.p1, 'b', d2.p1) AS s {from}",
                NarrowSql = $"INSERT INTO output SELECT f.id, f.k1, d1.a1 AS d1_a1, d2.a1 AS d2_a1 {from}"
            };
            workload.Shape["Multi Join"] = 1;
            workload.Computed.Add("s", new LmComputed(true, ["d1_p1", "d2_p1"]));
            return workload;
        }

        private static LmWorkload Group(string name, double scale, bool statefulMeasures)
        {
            long groups = Scaled(4_000_000, scale);
            var columns = new List<LmColumn>()
            {
                new LmColumn("id", LmColumnKind.Key),
                new LmColumn("gid", LmColumnKind.GroupId, modulus: groups),
                new LmColumn("amount", LmColumnKind.Number)
            };
            // A group key of about six hundred bytes
            for (int i = 1; i <= 4; i++)
            {
                columns.Add(new LmColumn($"g{i}", LmColumnKind.GroupText, 150, groups, true));
            }
            var fact = new LmTable("fact", Scaled(20_000_000, scale), true, columns.ToArray());
            string measures = statefulMeasures
                ? "count(*) AS c, sum(amount) AS s, min(amount) AS mn, count(DISTINCT amount) AS cd"
                : "count(*) AS c, sum(amount) AS s";
            var workload = new LmWorkload(name, [fact])
            {
                WideSql = $"INSERT INTO output SELECT g1, g2, g3, g4, {measures} FROM fact GROUP BY g1, g2, g3, g4",
                NarrowSql = $"INSERT INTO output SELECT gid, {measures} FROM fact GROUP BY gid",
                WideKeyColumns = 4,
                GroupColumn = "gid",
                // The group key stays stored once and in the sink
                KeptClasses = [LmCensus.Source, LmCensus.AggregateMain, LmCensus.Sink]
            };
            workload.Shape["Aggregation"] = 1;
            return workload;
        }

        private static LmWorkload FunctionalDependency(double scale)
        {
            var users = new LmTable("users", Scaled(2_000_000, scale), false,
                new LmColumn("id", LmColumnKind.Key),
                new LmColumn("a1", LmColumnKind.Number),
                new LmColumn("name", LmColumnKind.Text, 200, payload: true),
                new LmColumn("email", LmColumnKind.Text, 200, payload: true),
                new LmColumn("address", LmColumnKind.Text, 200, payload: true));
            var orders = new LmTable("orders", Scaled(20_000_000, scale), true,
                new LmColumn("id", LmColumnKind.Key),
                new LmColumn("userid", LmColumnKind.Reference, modulus: users.Rows),
                new LmColumn("amount", LmColumnKind.Number));
            string from = "FROM orders o INNER JOIN users u ON o.userid = u.id";
            string measures = "count(*) AS c, sum(o.amount) AS s, min(o.amount) AS mn, sum(u.a1) AS da";
            var workload = new LmWorkload("LM-FD", [orders, users])
            {
                // The user id alone is the sink's key in both forms
                WideSql = $"INSERT INTO output SELECT u.id, u.name, u.email, u.address, {measures} {from} GROUP BY u.id, u.name, u.email, u.address",
                NarrowSql = $"INSERT INTO output SELECT u.id, {measures} {from} GROUP BY u.id",
                GroupColumn = "userid"
            };
            workload.Shape["Aggregation"] = 1;
            workload.Shape["Merge Join Bulk"] = 1;
            return workload;
        }

        private static LmWorkload Partitioned(double scale)
        {
            var columns = new List<LmColumn>()
            {
                new LmColumn("id", LmColumnKind.Key),
                new LmColumn("a1", LmColumnKind.Number)
            };
            for (int i = 1; i <= 4; i++)
            {
                columns.Add(new LmColumn($"g{i}", LmColumnKind.Text, 150, payload: true));
            }
            var groups = new LmTable("gdim", Scaled(4_000_000, scale), false, columns.ToArray());
            var fact = new LmTable("fact", Scaled(20_000_000, scale), true,
                new LmColumn("id", LmColumnKind.Key),
                new LmColumn("gid", LmColumnKind.Reference, modulus: groups.Rows),
                new LmColumn("amount", LmColumnKind.Number));
            string from = "FROM fact f INNER JOIN gdim d ON f.gid = d.id";
            string measures = "count(*) AS c, sum(f.amount) AS s, sum(d.a1) AS da";
            var workload = new LmWorkload("LM-P", [fact, groups])
            {
                WideSql = $"INSERT INTO output SELECT d.g1, d.g2, d.g3, d.g4, {measures} {from} GROUP BY d.g1, d.g2, d.g3, d.g4",
                NarrowSql = $"INSERT INTO output SELECT d.id, {measures} {from} GROUP BY d.id",
                WideKeyColumns = 4,
                Parallelization = 4,
                GroupColumn = "gid",
                KeptClasses = [LmCensus.Source, LmCensus.AggregateMain, LmCensus.Sink]
            };
            workload.Shape["Aggregation"] = 4;
            workload.Shape["Merge Join Bulk"] = 4;
            return workload;
        }
    }
}
