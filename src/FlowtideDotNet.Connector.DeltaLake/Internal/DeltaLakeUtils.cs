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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal static class DeltaLakeUtils
    {
        public static string CreateFullLoadMergeIntoStatement(WriteRelation writeRelation, List<string> primaryKeyColumns)
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine("MERGE INTO mytable USING newdata");
            stringBuilder.Append("ON ");

            for (int i = 0; i < primaryKeyColumns.Count; i++)
            {
                if (i > 0)
                {
                    stringBuilder.Append(" AND ");
                }

                stringBuilder.Append("mytable.");
                stringBuilder.Append(primaryKeyColumns[i]);
                stringBuilder.Append(" = newdata.");
                stringBuilder.Append(primaryKeyColumns[i]);
            }

            stringBuilder.AppendLine();

            stringBuilder.AppendLine("WHEN MATCHED THEN");
            stringBuilder.AppendLine("  UPDATE SET");
            stringBuilder.AppendLine(
                string.Join(",\n", writeRelation.TableSchema.Names.Select(x => $"  {x} = newdata.{x}")));
            stringBuilder.AppendLine("WHEN NOT MATCHED BY SOURCE THEN DELETE");
            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET");
            stringBuilder.AppendLine("THEN INSERT (");
            stringBuilder.AppendLine(string.Join(",\n", writeRelation.TableSchema.Names.Select(x => $"  {x}")));
            stringBuilder.AppendLine(") VALUES (");
            stringBuilder.AppendLine(string.Join(",\n", writeRelation.TableSchema.Names.Select(x => $"  newdata.{x}")));
            stringBuilder.AppendLine(")");

            return stringBuilder.ToString();
        }

        public static string CreateUpsertMergeIntoStatement(WriteRelation writeRelation, List<string> primaryKeyColumns)
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine("MERGE INTO mytable USING newdata");
            stringBuilder.Append("ON ");

            for (int i = 0; i < primaryKeyColumns.Count; i++)
            {
                if (i > 0)
                {
                    stringBuilder.Append(" AND ");
                }

                stringBuilder.Append("mytable.");
                stringBuilder.Append(primaryKeyColumns[i]);
                stringBuilder.Append(" = newdata.");
                stringBuilder.Append(primaryKeyColumns[i]);
            }

            stringBuilder.AppendLine();

            stringBuilder.AppendLine("WHEN MATCHED THEN");
            stringBuilder.AppendLine("  DELETE");
            //stringBuilder.AppendLine("WHEN NOT MATCHED BY SOURCE AND newdata._flowtide_deleted = false THEN DELETE");
            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET");
            stringBuilder.AppendLine("THEN INSERT (");
            stringBuilder.AppendLine(string.Join(",\n", writeRelation.TableSchema.Names.Select(x => $"  {x}")));
            stringBuilder.AppendLine(") VALUES (");
            stringBuilder.AppendLine(string.Join(",\n", writeRelation.TableSchema.Names.Select(x => $"  newdata.{x}")));
            stringBuilder.AppendLine(")");

            stringBuilder.AppendLine("WHEN MATCHED AND newdata._flowtide_deleted = true THEN");
            stringBuilder.AppendLine("  UPDATE SET");
            stringBuilder.AppendLine(
                string.Join(",\n", writeRelation.TableSchema.Names.Select(x => $"  {x} = newdata.{x}")));

            return stringBuilder.ToString();
        }
    }
}
