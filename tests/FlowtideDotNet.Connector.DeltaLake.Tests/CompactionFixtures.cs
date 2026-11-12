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

using FlowtideDotNet.AcceptanceTests.Entities;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Where a fixture's deletes land
    internal enum DeletePlace
    {
        None,
        // The newest tenth of the keys
        Recent,
        Uniform
    }

    // A probe table, and what each commit inserts and deletes
    internal sealed record CompactionFixture(string Name, string Base, int BaseRows, int Inserts, int Deletes, int DeleteEvery, DeletePlace Place, int PreAge, bool DeletionVectors, bool RandomKey)
    {
        public static readonly IReadOnlyDictionary<string, CompactionFixture> All = new[]
        {
            new CompactionFixture("A", "ordered", 10_000_000, 2_000, 0, 1, DeletePlace.None, 0, true, false),
            new CompactionFixture("K", "ordered", 10_000_000, 2_000, 1_000, 1, DeletePlace.Recent, 1_000, true, false),
            new CompactionFixture("U", "ordered", 10_000_000, 2_000, 1_000, 1, DeletePlace.Uniform, 1_000, true, false),
            new CompactionFixture("M", "ordered", 10_000_000, 2_000, 50, 1, DeletePlace.Uniform, 0, true, false),
            new CompactionFixture("S", "ordered", 10_000_000, 2_000, 1, 20, DeletePlace.Uniform, 0, true, false),
            new CompactionFixture("Krand", "random", 1_000_000, 200, 100, 1, DeletePlace.Recent, 0, true, true),
            new CompactionFixture("DvoffK", "dvoff", 2_000_000, 400, 200, 1, DeletePlace.Recent, 500, false, false),
            // Scaled for the campaign, each with its own base
            new CompactionFixture("Kprime", "kprime", 2_500_000, 500, 250, 1, DeletePlace.Recent, 1_000, true, false),
            new CompactionFixture("Uprime", "uprime", 1_600_000, 2_000, 370, 1, DeletePlace.Uniform, 100, true, false),
            new CompactionFixture("Krandquarter", "random-quarter", 250_000, 200, 100, 1, DeletePlace.Recent, 0, true, true)
        }.ToDictionary(x => x.Name, StringComparer.Ordinal);

        public const string Table = "wide";

        // Name, type and value of each column, the key first
        private static readonly (string Name, string Type, string Value)[] s_columns = new[]
        {
            ("userkey", "INT", "userKey"),
            ("doublevalue", "DOUBLE", "doubleValue"),
            ("gender", "INT", "gender"),
            ("firstname", "STRING", "firstName"),
            ("lastname", "STRING", "lastName"),
            ("nullablestring", "STRING", "nullableString"),
            ("companyid", "STRING", "companyId"),
            ("visits", "INT", "visits"),
            ("trimmable", "STRING", "trimmableNullableString"),
            ("active", "BOOLEAN", "active"),
            ("birthdate", "TIMESTAMP", "birthDate"),
            ("managerkey", "INT", "managerKey"),
            ("fullname", "STRING", "firstName || ' ' || lastName"),
            ("email", "STRING", "concat(firstName, '.', lastName, '@example.com')"),
            ("label", "STRING", "concat(lastName, '-', companyId)"),
            ("code", "STRING", "concat('U', userKey)"),
            ("amount", "DECIMAL(18,4)", "CAST(doubleValue AS DECIMAL(18,4))"),
            ("visited", "TIMESTAMP", "timestamp_add('DAY', 1, birthDate)"),
            ("note", "STRING", "concat(firstName, ' visits ', visits)"),
            ("score", "DOUBLE", "doubleValue * 2")
        };

        // Twelve user fields and eight derived columns, about 100 B
        public string Sql()
        {
            var columns = s_columns.ToList();
            if (RandomKey)
            {
                // Keys leave the pruning columns, so every delete selects every file
                var key = columns[0];
                columns.RemoveAt(0);
                columns.Insert(columns.FindIndex(x => x.Name == "managerkey"), key);
            }
            return $@"
                CREATE TABLE {Table} ({string.Join(", ", columns.Select(x => $"{x.Name} {x.Type}"))});

                INSERT INTO {Table}
                SELECT {string.Join(", ", columns.Select(x => $"{x.Value} AS {x.Name}"))}
                FROM users";
        }

        public static IEnumerable<string> ColumnNames => s_columns.Select(x => x.Name);

        public static IEnumerable<(string Name, string Type)> Columns => s_columns.Select(x => (x.Name, x.Type));
    }

    // Rows made from the key alone, the same in every process
    internal static class ProbeRows
    {
        private static readonly string[] s_syllables = { "an", "bel", "cor", "dan", "el", "fen", "gar", "hal", "is", "jon", "kel", "lin", "mar", "nor", "os", "pen", "quin", "ros", "sel", "tor", "ul", "ven", "wil", "yan" };
        private static readonly string[] s_endings = { "son", "berg", "ton", "ley", "man", "ez", "ski", "er" };
        private static readonly string[] s_companies = { "Contoso", "Fabrikam", "Northwind", "Tailspin" };
        private static readonly DateTime s_born = new DateTime(1980, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static User Create(int key)
        {
            var a = Mix((ulong)key);
            var b = Mix(a);
            var c = Mix(b);
            var nullable = ((a >> 60) & 1) == 0 ? null : "value";
            return new User()
            {
                UserKey = key,
                Gender = (Gender)(int)(a & 1),
                FirstName = Name(a >> 1, 3, ""),
                LastName = Name(a >> 30, 2, s_endings[(int)((a >> 50) % (ulong)s_endings.Length)]),
                NullableString = nullable,
                TrimmableNullableString = nullable != null ? $" {nullable} " : null,
                CompanyId = (b & 1) == 0 ? null : s_companies[(int)((b >> 1) % (ulong)s_companies.Length)],
                Visits = (b >> 8) % 11 == 0 ? null : 1 + (int)((b >> 16) % 10),
                ManagerKey = key == 1 ? null : 1 + (int)((b >> 24) % (ulong)(key - 1)),
                DoubleValue = (c >> 11) * (1.0 / (1UL << 53)) * 1_000,
                Active = (c & 1) == 1,
                BirthDate = s_born.AddDays((double)((c >> 32) % 7_305))
            };
        }

        // Syllable names, spread about as widely as Bogus names
        private static string Name(ulong x, int syllables, string ending)
        {
            var name = new StringBuilder();
            for (int i = 0; i < syllables; i++)
            {
                name.Append(s_syllables[(int)(x % (ulong)s_syllables.Length)]);
                x /= (ulong)s_syllables.Length;
            }
            name.Append(ending);
            name[0] = char.ToUpperInvariant(name[0]);
            return name.ToString();
        }

        public static List<User> Create(IEnumerable<int> keys)
        {
            return keys.Select(Create).ToList();
        }

        // SplitMix64
        private static ulong Mix(ulong x)
        {
            x += 0x9E3779B97F4A7C15;
            x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9;
            x = (x ^ (x >> 27)) * 0x94D049BB133111EB;
            return x ^ (x >> 31);
        }
    }

    // Replays the same keys and deletes in every process
    internal sealed class CompactionWorkload
    {
        private const int Seed = 4_242;
        private readonly CompactionFixture _fixture;
        private readonly Random _random = new Random(Seed);
        private Queue<int>? _staggered;
        private bool[] _live = new bool[1_024];
        private int _maxKey;
        private int _liveRows;

        public CompactionWorkload(CompactionFixture fixture)
        {
            _fixture = fixture;
        }

        // Workload commits made so far, the base not counted
        public int Commits { get; private set; }

        public int LiveRows => _liveRows;

        // The base, in commits of the fixture's insert size
        public IEnumerable<List<User>> Base()
        {
            for (int rows = 0; rows < _fixture.BaseRows; rows += _fixture.Inserts)
            {
                var first = _maxKey + 1;
                var count = Math.Min(_fixture.Inserts, _fixture.BaseRows - rows);
                Insert(count);
                yield return ProbeRows.Create(Enumerable.Range(first, count));
            }
        }

        // The base and earlier commits, without making their rows
        public void Skip(int commits)
        {
            for (int rows = 0; rows < _fixture.BaseRows; rows += _fixture.Inserts)
            {
                Insert(Math.Min(_fixture.Inserts, _fixture.BaseRows - rows));
            }
            for (int i = 0; i < commits; i++)
            {
                NextKeys();
            }
        }

        public (List<User> Inserts, List<User> Deletes) Next()
        {
            var (first, deletes) = NextKeys();
            return (ProbeRows.Create(Enumerable.Range(first, _fixture.Inserts)), ProbeRows.Create(deletes));
        }

        // Deletes come first, so they never meet this commit's rows
        private (int First, List<int> Deletes) NextKeys()
        {
            var deletes = new List<int>();
            if (_fixture.Place == DeletePlace.Uniform && Commits < _fixture.PreAge)
            {
                var staggered = _staggered ??= Staggered();
                var count = (staggered.Count + _fixture.PreAge - Commits - 1) / (_fixture.PreAge - Commits);
                for (int i = 0; i < count; i++)
                {
                    deletes.Add(Take(staggered.Dequeue()));
                }
            }
            else if (_fixture.Place != DeletePlace.None && Commits % _fixture.DeleteEvery == 0)
            {
                for (int i = 0; i < _fixture.Deletes; i++)
                {
                    deletes.Add(TakeLive());
                }
            }
            var first = _maxKey + 1;
            Insert(_fixture.Inserts);
            Commits++;
            return (first, deletes);
        }

        private void Insert(int count)
        {
            if (_maxKey + count >= _live.Length)
            {
                Array.Resize(ref _live, Math.Max(_live.Length * 2, _maxKey + count + 1));
            }
            for (int key = _maxKey + 1; key <= _maxKey + count; key++)
            {
                _live[key] = true;
            }
            _maxKey += count;
            _liveRows += count;
        }

        // Each base file gets its own debt under 10 %, over the pre-age
        private Queue<int> Staggered()
        {
            var random = new Random(Seed + 1);
            var keys = new List<int>();
            for (int start = 1; start <= _fixture.BaseRows; start += _fixture.Inserts)
            {
                var rows = Math.Min(_fixture.Inserts, _fixture.BaseRows - start + 1);
                var target = (int)(random.NextDouble() * 0.10 * rows);
                var offsets = Enumerable.Range(0, rows).ToArray();
                random.Shuffle(offsets);
                keys.AddRange(offsets.Take(target).Select(x => start + x));
            }
            var shuffled = keys.ToArray();
            random.Shuffle(shuffled);
            return new Queue<int>(shuffled);
        }

        // A random live row in the fixture's range
        private int TakeLive()
        {
            var low = _fixture.Place == DeletePlace.Recent ? Math.Max(1, _maxKey - _liveRows / 10) : 1;
            while (true)
            {
                var key = _random.Next(low, _maxKey + 1);
                if (_live[key])
                {
                    return Take(key);
                }
            }
        }

        private int Take(int key)
        {
            _live[key] = false;
            _liveRows--;
            return key;
        }
    }
}
