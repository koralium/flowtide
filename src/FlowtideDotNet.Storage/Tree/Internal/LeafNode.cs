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

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal class LeafNode<K, V> : BaseNode<K>
    {

        public List<V> values;

        public long next;

        public LeafNode(long id) : base(id)
        {
            values = new List<V>();
        }

        public void InsertAt(in K key, in V value, in int position)
        {
            this.EnterWriteLock();
            keys.Insert(position, key);
            values.Insert(position, value);
            this.ExitWriteLock();
        }

        public void UpdateValueAt(in int index, in V value)
        {
            this.EnterWriteLock();
            values[index] = value;
            this.ExitWriteLock();
        }

        public void DeleteAt(in int index)
        {
            this.EnterWriteLock();
            keys.RemoveAt(index);
            values.RemoveAt(index);
            this.ExitWriteLock();
        }

        public override Task Print(StringBuilder stringBuilder, Func<long, ValueTask<BaseNode<K>>> lookupFunc)
        {
            stringBuilder.Append($"node{Id}");
            stringBuilder.Append($"[label = <");

            stringBuilder.Append("<table border=\"0\" cellborder=\"1\" cellspacing=\"0\">");
            stringBuilder.Append("<tr>");
            //stringBuilder.Append("<td port=\"f0\"></td>");

            for (int i = 0; i < keys.Count; i++)
            {
                stringBuilder.Append("<td>");
                stringBuilder.Append(keys[i]);
                stringBuilder.Append("</td>");
            }
            stringBuilder.Append("<td port=\"f0\" rowspan=\"2\"></td>");
            stringBuilder.Append("</tr>");

            // Print values
            stringBuilder.Append("<tr>");
            for (int i = 0; i < values.Count; i++)
            {
                stringBuilder.Append("<td>");
                stringBuilder.Append(values[i]);
                stringBuilder.Append("</td>");
            }
            stringBuilder.Append("</tr>");

            stringBuilder.Append("</table>");
            stringBuilder.AppendLine(">];");

            return Task.CompletedTask;
        }

        public override Task PrintNextPointers(StringBuilder stringBuilder)
        {
            if (next > 0)
            {
                stringBuilder.AppendLine($"\"node{Id}\":f1 -> \"node{next}\" [constraint=false];");
            }
            return Task.CompletedTask;
        }
    }
}
