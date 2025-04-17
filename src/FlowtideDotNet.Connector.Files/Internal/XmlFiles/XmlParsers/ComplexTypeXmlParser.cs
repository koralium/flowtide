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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace FlowtideDotNet.Connector.Files.Internal.XmlFiles.XmlParsers
{
    internal class ComplexTypeXmlParser : IFlowtideXmlParser
    {
        private readonly string elementName;
        private readonly Dictionary<string, int> elementsLookup;
        private readonly Dictionary<string, int> attributeLookup;
        private StructHeader header;
        private readonly IFlowtideXmlParser[] parsers;
        private readonly int[] listIndices;
        private readonly int[] propertyIndexToListIndex;

        public ComplexTypeXmlParser(
            string elementName, 
            string[] propertyNames, 
            IFlowtideXmlParser[] parsers,
            int[] listIndices,
            Dictionary<string, int> elementsLookup,
            Dictionary<string, int> attributeLookup)
        {
            this.elementName = elementName;
            this.parsers = parsers;
            this.listIndices = listIndices;
            this.elementsLookup = elementsLookup;
            this.attributeLookup = attributeLookup;
            propertyIndexToListIndex = new int[propertyNames.Length];
            for (int i = 0; i < propertyNames.Length; i++)
            {
                propertyIndexToListIndex[i] = -1;
            }
            for (int i = 0; i < listIndices.Length; i++)
            {
                propertyIndexToListIndex[listIndices[i]] = i;
            }

            header = StructHeader.Create(propertyNames);
        }

        public async ValueTask<IDataValue> Parse(XmlReader reader)
        {
            List<IDataValue>[] lists = new List<IDataValue>[listIndices.Length];
            IDataValue[] columns = new IDataValue[parsers.Length];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = NullValue.Instance;
            }

            while (reader.MoveToNextAttribute())
            {
                if (attributeLookup.TryGetValue(reader.LocalName, out var attributeIndex))
                {
                    columns[attributeIndex] = await parsers[attributeIndex].Parse(reader);
                }
            }

            reader.MoveToElement();

            if (reader.IsEmptyElement)
            {
                return new StructValue(header, columns);
            }

            while (await reader.ReadAsync())
            {
                if (reader.NodeType == XmlNodeType.Whitespace)
                {
                    continue;
                }
                if (reader.LocalName == elementName)
                {
                    break;
                }
                if (reader.NodeType == XmlNodeType.Element && elementsLookup.TryGetValue(reader.LocalName, out var propertyIndex))
                {
                    var listIndex = propertyIndexToListIndex[propertyIndex];
                    if (listIndex >= 0)
                    {
                        // If it is a list, add the value to a list index
                        if (lists[listIndex] == null)
                        {
                            lists[listIndex] = new List<IDataValue>();
                        }
                        lists[listIndex].Add(await parsers[propertyIndex].Parse(reader));
                    }
                    else
                    {
                        columns[propertyIndex] = await parsers[propertyIndex].Parse(reader);
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Unknown property {reader.LocalName}");
                }
            }

            for (int i = 0; i < lists.Length; i++)
            {
                if (lists[i] != null)
                {
                    columns[listIndices[i]] = new ListValue(lists[i]);
                }
                else
                {
                    columns[listIndices[i]] = new ListValue(Array.Empty<IDataValue>());
                }
            }

            return new StructValue(header, columns);
        }
    }
}
