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

using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Base.Metrics.Internal
{
    internal class FlowtideUpDownCounter<T> : IUpDownCounter<T>
        where T : struct
    {
        private readonly UpDownCounter<T> m_counter;
        private readonly TagList m_globalTags;

        public FlowtideUpDownCounter(UpDownCounter<T> counter, TagList globalTags)
        {
            this.m_counter = counter;
            this.m_globalTags = globalTags;
        }

        public void Add(T delta)
        {
            TagList tagList = new TagList();
            foreach (var tag in m_globalTags)
            {
                tagList.Add(tag);
            }
            m_counter.Add(delta, tagList);
        }

        public void Add(T delta, KeyValuePair<string, object?> tag)
        {
            TagList outputTags = new TagList();
            outputTags.Add(tag);
            foreach (var t in m_globalTags)
            {
                outputTags.Add(t);
            }
            m_counter.Add(delta, outputTags);
        }

        public void Add(T delta, KeyValuePair<string, object?> tag1, KeyValuePair<string, object?> tag2)
        {
            TagList outputTags = new TagList();
            outputTags.Add(tag1);
            outputTags.Add(tag2);
            foreach (var t in m_globalTags)
            {
                outputTags.Add(t);
            }
            m_counter.Add(delta, outputTags);
        }

        public void Add(T delta, KeyValuePair<string, object?> tag1, KeyValuePair<string, object?> tag2, KeyValuePair<string, object?> tag3)
        {
            TagList outputTags = new TagList();
            outputTags.Add(tag1);
            outputTags.Add(tag2);
            outputTags.Add(tag3);
            foreach (var t in m_globalTags)
            {
                outputTags.Add(t);
            }
            m_counter.Add(delta, outputTags);
        }

        public void Add(T delta, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            TagList outputTags = new TagList(tags);
            foreach (var t in m_globalTags)
            {
                outputTags.Add(t);
            }
            m_counter.Add(delta, outputTags);
        }

        public void Add(T delta, params KeyValuePair<string, object?>[] tags)
        {
            TagList outputTags = new TagList();
            foreach (var tag in tags)
            {
                outputTags.Add(tag);
            }
            foreach (var t in m_globalTags)
            {
                outputTags.Add(t);
            }
            m_counter.Add(delta, outputTags);
        }

        public void Add(T delta, in TagList tagList)
        {
            TagList outputTags = new TagList();
            foreach (var tag in tagList)
            {
                outputTags.Add(tag);
            }
            foreach (var t in m_globalTags)
            {
                outputTags.Add(t);
            }
            m_counter.Add(delta, outputTags);
        }
    }
}
