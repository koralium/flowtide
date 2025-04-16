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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Schema;
using System.Xml;
using FlowtideDotNet.Connector.Files.Internal.XmlFiles.XmlParsers;

namespace FlowtideDotNet.Connector.Files.Internal.XmlFiles
{
    public class SchemaToParsers
    {
        private Dictionary<XmlQualifiedName, XmlSchemaType>? schemaTypes;
        private Dictionary<XmlQualifiedName, XmlSchemaElement>? globalElements;

        public IFlowtideXmlParser ParseElement(string elementName, XmlSchemaSet schemaSet)
        {
            schemaSet.Compile();

            schemaTypes = new Dictionary<XmlQualifiedName, XmlSchemaType>();
            globalElements = new Dictionary<XmlQualifiedName, XmlSchemaElement>();

            foreach (XmlSchema schema in schemaSet.Schemas())
            {
                foreach (XmlSchemaObject item in schema.Items)
                {
                    if (item is XmlSchemaComplexType complexType)
                        schemaTypes[complexType.QualifiedName] = complexType;
                    else if (item is XmlSchemaSimpleType simpleType)
                        schemaTypes[simpleType.QualifiedName] = simpleType;
                    else if (item is XmlSchemaElement el)
                        globalElements[el.QualifiedName] = el;
                }
            }

            foreach (var kv in globalElements)
            {
                if (kv.Key.Name == elementName)
                {
                    return ParseElementInternal(kv.Value);
                }
            }
            throw new Exception($"Element not found: {elementName}");
        }

        private IFlowtideXmlParser ParseElementInternal(XmlSchemaElement element)
        {
            Debug.Assert(schemaTypes != null);
            Debug.Assert(globalElements != null);

            if (!element.RefName.IsEmpty)
            {
                if (globalElements.TryGetValue(element.RefName, out var referencedElement))
                    return ParseElementInternal(referencedElement);
                else
                    throw new Exception($"Referenced element not found: {element.RefName}");
            }

            XmlSchemaType? schemaType = element.ElementSchemaType;

            if (schemaType is XmlSchemaSimpleType simpleType)
            {
                return TypeCodeToParser(simpleType.TypeCode);
            }

            if (element.SchemaTypeName != null && !element.SchemaTypeName.IsEmpty)
            {
                if (schemaTypes.TryGetValue(element.SchemaTypeName, out var namedType))
                    schemaType = namedType;
                else
                    throw new Exception($"Type not found: {element.SchemaTypeName}");
            }

            if (schemaType is XmlSchemaComplexType complexType)
            {
                Dictionary<string, IFlowtideXmlParser> parsers = new Dictionary<string, IFlowtideXmlParser>();

                if (complexType.ContentTypeParticle is XmlSchemaSequence sequence)
                {

                    foreach (XmlSchemaObject item in sequence.Items)
                    {
                        if (item is XmlSchemaElement childElement)
                        {
                            // Check if this is a list or not
                            var parser = ParseElementInternal(childElement);
                            if (childElement.MaxOccurs > 1)
                            {
                                parser = new ListTypeXmlParser(childElement.Name!, parser);
                            }
                            parsers.Add(childElement.Name!, parser);
                        }
                    }
                }
                else if (complexType.ContentTypeParticle is XmlSchemaAll allSchema)
                {
                    foreach (var item in allSchema.Items)
                    {
                        if (item is XmlSchemaElement childElement)
                        {
                            var parser = ParseElementInternal(childElement);
                            if (childElement.MaxOccurs > 1)
                            {
                                parser = new ListTypeXmlParser(childElement.Name!, parser);
                            }
                            parsers[childElement.Name!] = parser;
                        }
                    }
                }

                // Handle attributes
                //foreach (XmlSchemaObject attrObj in complexType.Attributes)
                //{
                //    if (attrObj is XmlSchemaAttribute attribute)
                //    {
                //        names.Add(attribute.Name!);

                //        SubstraitBaseType type = AnyType.Instance;

                //        if (attribute.AttributeSchemaType != null)
                //        {
                //            type = TypeCodeToParser(attribute.AttributeSchemaType.TypeCode);
                //        }

                //        types.Add(type);
                //    }
                //}


                return new ComplexTypeXmlParser(parsers);
            }

            throw new NotImplementedException($"Type not implemented: {schemaType!.GetType()}");
        }

        private IFlowtideXmlParser TypeCodeToParser(XmlTypeCode typeCode)
        {

            return new StringXmlParser();
            //switch (typeCode)
            //{
            //    case XmlTypeCode.Boolean:
            //        return new BoolType();
            //    case XmlTypeCode.Decimal:
            //        return new DecimalType();
            //    case XmlTypeCode.Double:
            //    case XmlTypeCode.Float:
            //        return new Fp64Type();
            //    case XmlTypeCode.String:
            //    case XmlTypeCode.Text:
            //        return new StringType();
            //    case XmlTypeCode.Short:
            //    case XmlTypeCode.Int:
            //    case XmlTypeCode.Integer:
            //    case XmlTypeCode.NegativeInteger:
            //    case XmlTypeCode.NonNegativeInteger:
            //    case XmlTypeCode.UnsignedByte:
            //    case XmlTypeCode.UnsignedInt:
            //    case XmlTypeCode.UnsignedLong:
            //        return new Int64Type();
            //    case XmlTypeCode.Date:
            //    case XmlTypeCode.DateTime:
            //        return new TimestampType();
            //}
            //return new AnyType();
        }
    }
}
