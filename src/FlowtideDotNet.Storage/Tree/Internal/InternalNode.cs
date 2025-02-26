﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Text;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal class InternalNode<K, V, TKeyContainer> : BaseNode<K, TKeyContainer>
        where TKeyContainer : IKeyContainer<K>
    {
        public PrimitiveList<long> children;

        public InternalNode(long id, TKeyContainer keyContainer, IMemoryAllocator memoryAllocator) : base(id, keyContainer)
        {
            children = new PrimitiveList<long>(memoryAllocator);
        }

        public InternalNode(long id, TKeyContainer keyContainer, PrimitiveList<long> children) : base(id, keyContainer)
        {
            this.children = children;
        }

        public override async Task Print(StringBuilder stringBuilder, Func<long, ValueTask<BaseNode<K, TKeyContainer>>> lookupFunc)
        {
            stringBuilder.Append($"node{Id}");
            stringBuilder.Append($"[label = <");

            stringBuilder.Append("<table border=\"0\" cellborder=\"1\" cellspacing=\"0\">");
            stringBuilder.Append("<tr>");

            for (int i = 0; i < keys.Count; i++)
            {
                stringBuilder.Append($"<td port=\"f{i}\"></td>");
                stringBuilder.Append("<td>");
                stringBuilder.Append(keys.Get(i));
                stringBuilder.Append("</td>");
            }
            stringBuilder.Append($"<td port=\"f{keys.Count}\"></td>");

            stringBuilder.Append("</tr></table>");
            stringBuilder.AppendLine(">];");

            for (int i = 0; i < children.Count; i++)
            {
                var childId = children[i];
                var node = await lookupFunc(childId);
                await node.Print(stringBuilder, lookupFunc);
                stringBuilder.AppendLine($"\"node{Id}\":f{i} -> \"node{childId}\"");
            }

            for (int i = 0; i < children.Count; i++)
            {
                var childId = children[i];
                var node = await lookupFunc(childId);
                await node.PrintNextPointers(stringBuilder);
            }
        }

        public override Task PrintNextPointers(StringBuilder stringBuilder)
        {
            return Task.CompletedTask;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                children.Dispose();
            }
            base.Dispose(disposing);
        }

        public override int GetByteSize()
        {
            return keys.GetByteSize() + children.Count * sizeof(long);
        }

        public override int GetByteSize(int start, int end)
        {
            return keys.GetByteSize(start, end) + (end - start + 1) * sizeof(long);
        }
    }
}
