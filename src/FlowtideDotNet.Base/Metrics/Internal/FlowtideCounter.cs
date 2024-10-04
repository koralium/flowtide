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

using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Base.Metrics.Internal
{
    internal class FlowtideCounter<T> : ICounter<T>
        where T : struct
    {
        private readonly Counter<T> m_counter;
        private readonly KeyValuePair<string, object?>[] m_globalTags;

        public FlowtideCounter(Counter<T> counter, TagList globalTags)
        {
            this.m_counter = counter;
            this.m_globalTags = globalTags.ToArray();
        }

        public void Add(T delta)
        {
            m_counter.Add(delta, m_globalTags);
        }

        public void Add(T delta, KeyValuePair<string, object?> tag)
        {
            TagList tagList = new TagList();
            tagList.Add(tag);
            foreach (var t in m_globalTags)
            {
                tagList.Add(t);
            }
            m_counter.Add(delta, tagList);
        }

        public void Add(T delta, KeyValuePair<string, object?> tag1, KeyValuePair<string, object?> tag2)
        {
            TagList tagList = new TagList();
            tagList.Add(tag1);
            tagList.Add(tag2);
            foreach (var tag in m_globalTags)
            {
                tagList.Add(tag);
            }
            m_counter.Add(delta, tagList);
        }

        public void Add(T delta, KeyValuePair<string, object?> tag1, KeyValuePair<string, object?> tag2, KeyValuePair<string, object?> tag3)
        {
            TagList tagList = new TagList();
            tagList.Add(tag1);
            tagList.Add(tag2);
            tagList.Add(tag3);
            foreach (var tag in m_globalTags)
            {
                tagList.Add(tag);
            }
            m_counter.Add(delta, tagList);
        }

        public void Add(T delta, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            TagList tagList = new TagList(tags);
            foreach (var tag in m_globalTags)
            {
                tagList.Add(tag);
            }
            m_counter.Add(delta, tagList);
        }

        public void Add(T delta, params KeyValuePair<string, object?>[] tags)
        {
            TagList tagList = new TagList();
            foreach(var tag in tags)
            {
                tagList.Add(tag);
            }
            foreach (var tag in m_globalTags)
            {
                tagList.Add(tag);
            }
            m_counter.Add(delta, tagList);
        }

        public void Add(T delta, in TagList tagList)
        {
            TagList outputList = new TagList();
            foreach (var tag in tagList)
            {
                outputList.Add(tag);
            }
            foreach (var tag in m_globalTags)
            {
                outputList.Add(tag);
            }
            m_counter.Add(delta, outputList);
        }
    }
}
