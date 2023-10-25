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

using SqlParser;
using SqlParser.Ast;

namespace FlowtideDotNet.Substrait.Sql
{
    public class EmitData
    {
        private sealed class EmitInformation
        {
            public List<int> Index { get; }

            public EmitInformation()
            {
                Index = new List<int>();
            }
        }

        private readonly Dictionary<Expression, EmitInformation> emitList;
        private SortedDictionary<string, Expression.CompoundIdentifier> compundIdentifiers;
        private List<string> _names;

        public EmitData()
        {
            emitList = new Dictionary<Expression, EmitInformation>();
            compundIdentifiers = new SortedDictionary<string, Expression.CompoundIdentifier>(StringComparer.OrdinalIgnoreCase);
            _names = new List<string>();
        }

        public EmitData ClonewithAlias(string alias)
        {
            var clone = new EmitData();
            foreach (var kv in emitList)
            {
                clone.emitList.Add(kv.Key, kv.Value);
            }
            foreach (var kv in compundIdentifiers)
            {
                clone.compundIdentifiers.Add(kv.Key, kv.Value);
            }
            foreach (var name in _names)
            {
                clone._names.Add(name);
            }
            foreach (var ci in compundIdentifiers)
            {
                if (emitList.TryGetValue(ci.Value, out var indexInfo))
                {
                    clone.AddWithAlias(new Expression.CompoundIdentifier(new Sequence<Ident>(ci.Value.Idents.Prepend(new Ident(alias)))), indexInfo.Index.First());
                }
                else
                {
                    throw new InvalidOperationException("Could not find index information");
                }
            }
            return clone;
        }

        public void Add(EmitData left, int offset)
        {
            foreach (var kv in left.emitList)
            {
                if (!emitList.TryGetValue(kv.Key, out var val))
                {
                    val = new EmitInformation();
                    emitList[kv.Key] = val;
                }
                foreach (var index in kv.Value.Index)
                {
                    val.Index.Add(index + offset);
                }
            }
            foreach (var compoundIdentifier in left.compundIdentifiers)
            {
                if (!compundIdentifiers.ContainsKey(compoundIdentifier.Key))
                {
                    compundIdentifiers.Add(compoundIdentifier.Key, compoundIdentifier.Value);
                }
            }
            foreach (var name in left._names)
            {
                _names.Add(name);
            }
        }

        public void Add(Expression expr, int index, string name)
        {
            if (expr is Expression.CompoundIdentifier compound)
            {
                var k = string.Join(".", compound.Idents.Select(x => x.Value));
                if (!compundIdentifiers.ContainsKey(k))
                {
                    compundIdentifiers.Add(string.Join(".", compound.Idents.Select(x => x.Value)), compound);
                }
            }

            if (!emitList.TryGetValue(expr, out var existingIndex))
            {
                existingIndex = new EmitInformation();
                emitList.Add(expr, existingIndex);
            }
            _names.Insert(index, name);
            existingIndex.Index.Add(index);
        }

        public void AddWithAlias(Expression expr, int index)
        {
            if (expr is Expression.CompoundIdentifier compound)
            {
                compundIdentifiers.Add(string.Join(".", compound.Idents.Select(x => x.Value)), compound);
            }

            if (!emitList.TryGetValue(expr, out var existingIndex))
            {
                existingIndex = new EmitInformation();
                emitList.Add(expr, existingIndex);
            }
            existingIndex.Index.Add(index);
        }

        public bool TryGetEmitIndex(Expression expression, out int index)
        {
            if (emitList.TryGetValue(expression, out var emitInfo))
            {
                if (emitInfo.Index.Count > 1)
                {
                    throw new InvalidOperationException($"Multiple matches for expression: '{expression.ToSql()}'");
                }
                index = emitInfo.Index.First();
                return true;
            }

            // If it is a compound identifier, we can try to look for it with case insensitive lookup
            if (expression is Expression.CompoundIdentifier compoundIndentifier)
            {
                var textString = compoundIndentifier.ToSql();
                if (compundIdentifiers.TryGetValue(textString, out var compoundIdentifier))
                {
                    if (emitList.TryGetValue(compoundIdentifier, out var emitInfo2))
                    {
                        if (emitInfo2.Index.Count > 1)
                        {
                            throw new InvalidOperationException($"Multiple matches for expression: '{expression.ToSql()}'");
                        }
                        index = emitInfo2.Index.First();
                        return true;
                    }
                }
            }

            index = -1;
            return false;
        }

        public string GetName(int index)
        {
            return _names[index];
        }

        public List<string> GetNames()
        {
            return _names.ToList();
        }
    }
}
