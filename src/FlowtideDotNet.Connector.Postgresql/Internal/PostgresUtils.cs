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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Npgsql;
using Polly;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    /// <summary>
    /// Helpers for schema discovery, type mapping, value conversion and prerequisite validation against PostgreSQL.
    /// </summary>
    internal static class PostgresUtils
    {
        /// <summary>
        /// A marker placed in a decoded row for a TOAST column whose value was not included in an UPDATE because it
        /// did not change. The delta source must backfill the value (or trigger a re-snapshot) before emitting.
        /// </summary>
        public static readonly object UnchangedToast = new object();

        /// <summary>
        /// Maps a PostgreSQL base type name (udt_name / data type id name) to a Substrait type.
        /// </summary>
        public static SubstraitBaseType GetSubstraitType(string pgTypeName)
        {
            switch (pgTypeName.ToLowerInvariant())
            {
                case "bool":
                case "boolean":
                    return new BoolType();
                case "int2":
                case "smallint":
                case "int4":
                case "integer":
                case "int8":
                case "bigint":
                case "oid":
                    return new Int64Type();
                case "float4":
                case "real":
                case "float8":
                case "double precision":
                    return new Fp64Type();
                case "numeric":
                case "decimal":
                case "money":
                    return new DecimalType();
                case "bytea":
                    return new BinaryType();
                case "date":
                case "timestamp":
                case "timestamptz":
                case "timestamp without time zone":
                case "timestamp with time zone":
                    return new TimestampType();
                case "time":
                case "timetz":
                case "time without time zone":
                case "time with time zone":
                    return new Int64Type();
                case "bpchar":
                case "char":
                case "character":
                case "varchar":
                case "character varying":
                case "text":
                case "name":
                case "citext":
                case "uuid":
                case "json":
                case "jsonb":
                case "xml":
                    return new StringType();
                default:
                    return new AnyType();
            }
        }

        /// <summary>
        /// Builds a converter for a column that turns a value into the canonical Flowtide data value, accepting both
        /// the typed CLR values produced by a data reader (snapshot) and the raw text values produced by the pgoutput
        /// replication stream (delta). This keeps the two paths consistent so primary-key lookups match.
        /// </summary>
        public static Action<IColumn, object?> BuildColumnConverter(string pgTypeName)
        {
            switch (pgTypeName.ToLowerInvariant())
            {
                case "bool":
                case "boolean":
                    return (c, v) => { if (IsNull(v)) { c.Add(NullValue.Instance); } else { c.Add(new BoolValue(ToBool(v!))); } };
                case "int2":
                case "smallint":
                case "int4":
                case "integer":
                case "int8":
                case "bigint":
                case "oid":
                    return (c, v) => { if (IsNull(v)) { c.Add(NullValue.Instance); } else { c.Add(new Int64Value(ToLong(v!))); } };
                case "float4":
                case "real":
                case "float8":
                case "double precision":
                    return (c, v) => { if (IsNull(v)) { c.Add(NullValue.Instance); } else { c.Add(new DoubleValue(ToDouble(v!))); } };
                case "numeric":
                case "decimal":
                case "money":
                    return (c, v) => { if (IsNull(v)) { c.Add(NullValue.Instance); } else { c.Add(new DecimalValue(ToDecimal(v!))); } };
                case "bytea":
                    return (c, v) => { if (IsNull(v)) { c.Add(NullValue.Instance); } else { c.Add(new BinaryValue(ToBytes(v!))); } };
                case "date":
                case "timestamp":
                case "timestamptz":
                case "timestamp without time zone":
                case "timestamp with time zone":
                    return (c, v) => { if (IsNull(v)) { c.Add(NullValue.Instance); } else { c.Add(new TimestampTzValue(ToTimestamp(v!))); } };
                case "time":
                case "timetz":
                case "time without time zone":
                case "time with time zone":
                    return (c, v) => { if (IsNull(v)) { c.Add(NullValue.Instance); } else { c.Add(new Int64Value(ToTicks(v!))); } };
                default:
                    return (c, v) => { if (IsNull(v)) { c.Add(NullValue.Instance); } else { c.Add(new StringValue(ToStringValue(v!))); } };
            }
        }

        private static bool IsNull(object? v) => v is null || v is DBNull;

        private static long ToLong(object v) => v is string s ? long.Parse(s, CultureInfo.InvariantCulture) : Convert.ToInt64(v, CultureInfo.InvariantCulture);

        private static double ToDouble(object v) => v is string s ? double.Parse(s, CultureInfo.InvariantCulture) : Convert.ToDouble(v, CultureInfo.InvariantCulture);

        private static decimal ToDecimal(object v) => v is string s ? decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture) : Convert.ToDecimal(v, CultureInfo.InvariantCulture);

        private static bool ToBool(object v) => v is bool b ? b : (v is string s && (s == "t" || s == "1" || s.Equals("true", StringComparison.OrdinalIgnoreCase) || s.Equals("yes", StringComparison.OrdinalIgnoreCase)));

        private static byte[] ToBytes(object v)
        {
            if (v is byte[] b)
            {
                return b;
            }
            var s = (string)v;
            return s.StartsWith("\\x", StringComparison.Ordinal) ? Convert.FromHexString(s.Substring(2)) : Encoding.UTF8.GetBytes(s);
        }

        private static DateTime ToTimestamp(object v)
        {
            switch (v)
            {
                case DateTime dt:
                    return dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt.ToUniversalTime();
                case DateTimeOffset dto:
                    return dto.UtcDateTime;
                case DateOnly d:
                    return DateTime.SpecifyKind(d.ToDateTime(TimeOnly.MinValue), DateTimeKind.Utc);
                default:
                    return DateTime.Parse((string)v, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
            }
        }

        private static long ToTicks(object v)
        {
            switch (v)
            {
                case TimeSpan ts:
                    return ts.Ticks;
                case TimeOnly to:
                    return to.Ticks;
                default:
                    var s = (string)v;
                    return TimeOnly.TryParse(s, CultureInfo.InvariantCulture, out var parsed) ? parsed.Ticks : TimeSpan.Parse(s, CultureInfo.InvariantCulture).Ticks;
            }
        }

        private static string ToStringValue(object v) => v as string ?? v.ToString() ?? string.Empty;

        public static string QuoteIdentifier(string identifier)
        {
            return "\"" + identifier.Replace("\"", "\"\"") + "\"";
        }

        public static string QualifiedName(string schema, string table)
        {
            return QuoteIdentifier(schema) + "." + QuoteIdentifier(table);
        }

        public static string BuildSnapshotSelect(string schema, string table, IReadOnlyList<string> columnNames, IReadOnlyList<string> orderByColumns)
        {
            var sb = new StringBuilder();
            sb.Append("SELECT ");
            sb.Append(string.Join(", ", columnNames.Select(QuoteIdentifier)));
            sb.Append(" FROM ");
            sb.Append(QualifiedName(schema, table));
            if (orderByColumns.Count > 0)
            {
                sb.Append(" ORDER BY ");
                sb.Append(string.Join(", ", orderByColumns.Select(QuoteIdentifier)));
            }
            return sb.ToString();
        }

        /// <summary>
        /// Resolves the schema and table name from a read relation's named table.
        /// Supports [table] (defaults schema to public), [schema, table] and [database, schema, table].
        /// </summary>
        public static (string schema, string table) ResolveSchemaAndTable(IReadOnlyList<string> tableNameParts)
        {
            switch (tableNameParts.Count)
            {
                case 1:
                    return ("public", tableNameParts[0]);
                case 2:
                    return (tableNameParts[0], tableNameParts[1]);
                case 3:
                    return (tableNameParts[1], tableNameParts[2]);
                default:
                    throw new InvalidOperationException("PostgreSQL table name must be [table], [schema.table] or [database.schema.table].");
            }
        }

        /// <summary>
        /// Opens a connection through the configured resilience pipeline so transient connection failures are retried.
        /// </summary>
        public static async ValueTask OpenWithResilienceAsync(NpgsqlConnection connection, ResiliencePipeline pipeline, CancellationToken ct)
        {
            await pipeline.ExecuteAsync(async token => await connection.OpenAsync(token), ct);
        }

        public static async Task<string> GetWalLevel(NpgsqlConnection connection, CancellationToken ct)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SHOW wal_level";
            var result = await cmd.ExecuteScalarAsync(ct);
            return (string?)result ?? string.Empty;
        }

        public static async Task<bool> HasReplicationPermission(NpgsqlConnection connection, CancellationToken ct)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user";
            var result = await cmd.ExecuteScalarAsync(ct);
            return result is bool b && b;
        }

        public static async Task<List<(string name, string udtName)>> GetColumns(NpgsqlConnection connection, string schema, string table, CancellationToken ct)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"SELECT column_name, udt_name
                                FROM information_schema.columns
                                WHERE table_schema = @schema AND table_name = @table
                                ORDER BY ordinal_position";
            cmd.Parameters.AddWithValue("schema", schema);
            cmd.Parameters.AddWithValue("table", table);
            var output = new List<(string, string)>();
            using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                output.Add((reader.GetString(0), reader.GetString(1)));
            }
            return output;
        }

        public static async Task<List<string>> GetPrimaryKeys(NpgsqlConnection connection, string schema, string table, CancellationToken ct)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"SELECT a.attname
                                FROM pg_index i
                                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                                WHERE i.indrelid = (quote_ident(@schema) || '.' || quote_ident(@table))::regclass
                                  AND i.indisprimary
                                ORDER BY array_position(i.indkey, a.attnum)";
            cmd.Parameters.AddWithValue("schema", schema);
            cmd.Parameters.AddWithValue("table", table);
            var output = new List<string>();
            using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                output.Add(reader.GetString(0));
            }
            return output;
        }

        /// <summary>
        /// Returns the table's replica identity setting: 'd' (default/primary key), 'n' (nothing), 'f' (full), 'i' (index).
        /// </summary>
        public static async Task<char> GetReplicaIdentity(NpgsqlConnection connection, string schema, string table, CancellationToken ct)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT relreplident FROM pg_class WHERE oid = (quote_ident(@schema) || '.' || quote_ident(@table))::regclass";
            cmd.Parameters.AddWithValue("schema", schema);
            cmd.Parameters.AddWithValue("table", table);
            var result = await cmd.ExecuteScalarAsync(ct);
            return result is char c ? c : (result is string s && s.Length > 0 ? s[0] : 'd');
        }

        public static async Task<List<(string schema, string table)>> GetAllTables(NpgsqlConnection connection, CancellationToken ct)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"SELECT table_schema, table_name
                                FROM information_schema.tables
                                WHERE table_type = 'BASE TABLE'
                                  AND table_schema NOT IN ('pg_catalog', 'information_schema')";
            var output = new List<(string, string)>();
            using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                output.Add((reader.GetString(0), reader.GetString(1)));
            }
            return output;
        }

        /// <summary>
        /// Creates a deterministic, length-bounded identifier (slot or publication) for the given inputs.
        /// PostgreSQL identifiers are limited to 63 bytes, so a hash suffix is used for uniqueness.
        /// </summary>
        public static string BuildIdentifier(string prefix, params string[] parts)
        {
            var raw = string.Join("_", parts);
            var hashBytes = SHA1.HashData(Encoding.UTF8.GetBytes(raw));
            var hash = Convert.ToHexString(hashBytes, 0, 6).ToLowerInvariant();
            var sanitized = new string(raw.ToLowerInvariant().Select(ch => char.IsLetterOrDigit(ch) ? ch : '_').ToArray());
            var maxBody = 63 - prefix.Length - hash.Length - 2;
            if (maxBody < 0)
            {
                maxBody = 0;
            }
            if (sanitized.Length > maxBody)
            {
                sanitized = sanitized.Substring(0, maxBody);
            }
            return $"{prefix}_{sanitized}_{hash}";
        }
    }
}
