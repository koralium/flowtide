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

using System.Buffers;
using System.Xml;

namespace FlexBuffers
{
    public static class XmlToFlexBufferConverter
    {
        public static byte[] Convert(string xmlData)
        {
            XmlDocument doc = new XmlDocument();
            
            doc.Load(new StringReader(xmlData));

            var flx = new FlexBuffer(ArrayPool<byte>.Shared);
            flx.NewObject();

            Process(flx, doc.DocumentElement);
            
            return flx.Finish();
        }

        private static void Process(FlexBuffer flx, XmlNode element)
        {
            var node = flx.StartVector();
            flx.AddKey("tagName");
            flx.Add(element.Name);
            var attributes = element.Attributes;
            if (attributes != null)
            {
                for (var i = 0; i < attributes.Count; i++)
                {
                    var att = attributes.Item(i);
                    flx.AddKey(att.Name);
                    flx.Add(att.Value);
                }
            }

            var children = element.ChildNodes;
            if (children.Count > 0)
            {
                flx.AddKey("children");
                var childVector = flx.StartVector();
                for (var i = 0; i < children.Count; i++)
                {
                    var child = children[i];
                    if (child.NodeType == XmlNodeType.Text || child.NodeType == XmlNodeType.CDATA)
                    {
                        flx.Add(child.Value);
                    } else if (child.NodeType == XmlNodeType.Comment)
                    {
                        
                    } else
                    {
                        Process(flx, child);    
                    }
                }

                flx.EndVector(childVector, false, false);
            }
            flx.SortAndEndMap(node);
        }
    }
}