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

using FlowtideDotNet.Substrait.Type;
using SqlParser;
using SqlParser.Ast;
using System.Diagnostics.CodeAnalysis;

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
        private List<SubstraitBaseType> _types;

        public EmitData()
        {
            emitList = new Dictionary<Expression, EmitInformation>();
            compundIdentifiers = new SortedDictionary<string, Expression.CompoundIdentifier>(StringComparer.OrdinalIgnoreCase);
            _names = new List<string>();
            _types = new List<SubstraitBaseType>();
        }

        public EmitData Clone()
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
            foreach (var type in _types)
            {
                clone._types.Add(type);
            }
            return clone;
        }

        public EmitData CloneWithAlias(string alias, List<string>? columnNames)
        {
            var clone = new EmitData();

            if (columnNames != null)
            {
                for (int index = 0; index < columnNames.Count; index++)
                {
                    var name = columnNames[index];
                    var emitInfo = new EmitInformation();
                    emitInfo.Index.Add(index);
                    clone.emitList.Add(new Expression.CompoundIdentifier(new Sequence<Ident>(new List<Ident> { new Ident(name) })), emitInfo);
                }

                foreach (var name in columnNames)
                {
                    clone.compundIdentifiers.Add(name, new Expression.CompoundIdentifier(new Sequence<Ident>(new List<Ident> { new Ident(name) })));
                }

                clone._names.AddRange(columnNames);
            }
            else
            {
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
            }

            foreach (var type in _types)
            {
                clone._types.Add(type);
            }
            if (columnNames != null)
            {
                for (int index = 0; index < columnNames.Count; index++)
                {
                    var name = columnNames[index];
                    clone.AddWithAlias(new Expression.CompoundIdentifier(new Sequence<Ident>(new List<Ident>() { new Ident(alias), new Ident(name) })), index);
                }
            }
            else
            {
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
            foreach (var type in left._types)
            {
                _types.Add(type);
            }
        }

        public void Add(Expression expr, int index, string name, SubstraitBaseType type)
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
            _types.Insert(index, type);
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

        public bool TryGetEmitIndex(
            Expression expression, 
            [NotNullWhen(true)] out Expressions.StructReferenceSegment? segment, 
            [NotNullWhen(true)] out string? name,
            [NotNullWhen(true)] out SubstraitBaseType? type)
        {
            if (emitList.TryGetValue(expression, out var emitInfo))
            {
                if (emitInfo.Index.Count > 1)
                {
                    throw new InvalidOperationException($"Multiple matches for expression: '{expression.ToSql()}'");
                }
                segment = new Expressions.StructReferenceSegment()
                {
                    Field = emitInfo.Index[0]
                };
                name = GetName(emitInfo.Index[0]);
                type = _types[emitInfo.Index[0]];
                return true;
            }

            // If it is a compound identifier, we can try to look for it with case insensitive lookup
            if (expression is Expression.CompoundIdentifier compoundIndentifier)
            {
                var textString = compoundIndentifier.ToSql();
                if (compundIdentifiers.TryGetValue(textString, out var compoundIdentifierResult) &&
                    emitList.TryGetValue(compoundIdentifierResult, out var emitInfo2))
                {
                    if (emitInfo2.Index.Count > 1)
                    {
                        throw new InvalidOperationException($"Multiple matches for expression: '{expression.ToSql()}'");
                    }
                    segment = new Expressions.StructReferenceSegment()
                    {
                        Field = emitInfo2.Index[0]
                    };
                    name = GetName(emitInfo2.Index[0]);
                    type = _types[emitInfo2.Index[0]];
                    return true;
                }
                // Try and iterate each property of the compound identifier, if found add remainder as map key reference
                for (int i = compoundIndentifier.Idents.Count; i >= 0; i--)
                {
                    var txt = string.Join(".", compoundIndentifier.Idents.Take(i).Select(x => x.Value));
                    if (compundIdentifiers.TryGetValue(txt, out var indentifier) &&
                        emitList.TryGetValue(indentifier, out var emitInfoPartial))
                    {
                        if (emitInfoPartial.Index.Count > 1)
                        {
                            throw new InvalidOperationException($"Multiple matches for expression: '{expression.ToSql()}'");
                        }
                        segment = new Expressions.StructReferenceSegment()
                        {
                            Field = emitInfoPartial.Index[0]
                        };

                        Expressions.ReferenceSegment seg = segment;
                        List<string> mapIdentifiers = new List<string>();
                        for (int k = i; k < compoundIndentifier.Idents.Count; k++)
                        {
                            var newSegment = new Expressions.MapKeyReferenceSegment()
                            {
                                Key = compoundIndentifier.Idents[k].Value,
                            };
                            mapIdentifiers.Add(compoundIndentifier.Idents[k].Value);
                            seg.Child = newSegment;
                            seg = newSegment;
                        }

                        if (mapIdentifiers.Count > 0)
                        {
                            var lastIdentifier = mapIdentifiers.Last();
                            name = lastIdentifier; // Use the last identifier as name
                        }
                        else
                        {
                            name = GetName(emitInfoPartial.Index[0]);
                        }
                        

                        // TODO: For now we just return any type, but we should try to find the correct type
                        type = new AnyType();
                        return true;
                    }
                }
            }

            segment = null;
            name = null;
            type = null;
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

        public List<SubstraitBaseType> GetTypes()
        {
            return _types.ToList();
        }

        public NamedStruct GetNamedStruct()
        {
            var namedStruct = new NamedStruct()
            {
                Names = _names.ToList(),
                Struct = new Struct()
                {
                    Types = _types.ToList()
                }
            };
            return namedStruct;
        }

        public record ExpressionInformation(int Index, string Name, IReadOnlyList<Expression> Expression, SubstraitBaseType Type);

        private static bool ExpressionStartsWith(Expression expression, ObjectName? objectName)
        {
            if (objectName == null)
            {
                return true;
            }
            if (expression is Expression.CompoundIdentifier compoundIdentifier)
            {
                return compoundIdentifier.Idents.Count >= objectName.Values.Count &&
                    compoundIdentifier.Idents.Take(objectName.Values.Count).Select(x => x.Value).SequenceEqual(objectName.Values.Select(x => x.Value));
            }
            return false;
        }

        public IReadOnlyList<ExpressionInformation> GetExpressions(ObjectName? objectName = null)
        {
            var indicesToExpression = emitList
                .Where(x => ExpressionStartsWith(x.Key, objectName))
                .SelectMany(x => x.Value.Index.Select(y => new { index = y, expression = x.Key }))
                .GroupBy(x => x.index)
                .Select(x => new ExpressionInformation(x.Key, GetName(x.Key), x.Select(z => z.expression).ToList(), _types[x.Key]))
                .OrderBy(x => x.Index)
                .ToList();

            return indicesToExpression;
        }

        public int Count => _names.Count;
    }
}
