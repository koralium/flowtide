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
    internal class DateTimeElementParser : IFlowtideXmlParser
    {
        public async ValueTask<IDataValue> Parse(XmlReader reader)
        {
            if (reader.IsEmptyElement)
            {
                return NullValue.Instance;
            }
            await reader.ReadAsync();

            IDataValue? returnVal = default;
            switch (reader.NodeType)
            {
                case XmlNodeType.Text:
                    if (DateTimeOffset.TryParse(reader.Value, out var i))
                    {
                        returnVal = new TimestampTzValue(i);
                    }
                    else
                    {
                        returnVal = NullValue.Instance;
                    }
                    break;
                default:
                    throw new NotSupportedException($"Unsupported node type: {reader.NodeType} for DateTimeElementParser");
            }
            await reader.ReadAsync();
            if (reader.NodeType == XmlNodeType.EndElement)
            {
                return returnVal;
            }
            else
            {
                throw new NotSupportedException($"Unsupported node type: {reader.NodeType} for StringXmlParser");
            }
        }
    }
}
