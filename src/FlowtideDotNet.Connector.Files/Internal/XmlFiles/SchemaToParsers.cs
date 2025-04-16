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
                Dictionary<string, int> lookup = new Dictionary<string, int>();
                Dictionary<string, int> attributeLookup = new Dictionary<string, int>();
                List<string> propertyNames = new List<string>();
                List<IFlowtideXmlParser> parsers = new List<IFlowtideXmlParser>();
                List<int> listIndices = new List<int>();

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
                                listIndices.Add(parsers.Count);
                            }
                            lookup.Add(childElement.Name!, parsers.Count);
                            parsers.Add(parser);
                            propertyNames.Add(childElement.Name!);
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
                                listIndices.Add(parsers.Count);
                            }
                            lookup[childElement.Name!] = parsers.Count;
                            parsers.Add(parser);
                            propertyNames.Add(childElement.Name!);
                        }
                    }
                }

                // Handle attributes
                foreach (XmlSchemaObject attrObj in complexType.Attributes)
                {
                    if (attrObj is XmlSchemaAttribute attribute)
                    {
                        propertyNames.Add(attribute.Name!);

                        if (attribute.AttributeSchemaType != null)
                        {
                            var parser = AttributeTypeCodeToParser(attribute.AttributeSchemaType.TypeCode);
                            attributeLookup.Add(attribute.Name!, parsers.Count);
                            parsers.Add(parser);
                        }
                        else
                        {
                            throw new InvalidOperationException($"Attribute {attribute.Name} does not have a schema type");
                        }
                    }
                }


                return new ComplexTypeXmlParser(
                    element.Name!, 
                    propertyNames.ToArray(), 
                    parsers.ToArray(), 
                    listIndices.ToArray(), 
                    lookup, 
                    attributeLookup);
            }

            throw new NotImplementedException($"Type not implemented: {schemaType!.GetType()}");
        }

        private IFlowtideXmlParser AttributeTypeCodeToParser(XmlTypeCode typeCode)
        {
            switch (typeCode)
            {
                case XmlTypeCode.String:
                case XmlTypeCode.Text:
                    return new StringAttributeParser();
                case XmlTypeCode.UnsignedByte:
                case XmlTypeCode.UnsignedInt:
                case XmlTypeCode.UnsignedLong:
                case XmlTypeCode.UnsignedShort:
                case XmlTypeCode.Byte:
                case XmlTypeCode.Int:
                case XmlTypeCode.Integer:
                case XmlTypeCode.Long:
                case XmlTypeCode.Short:
                case XmlTypeCode.PositiveInteger:
                case XmlTypeCode.NonNegativeInteger:
                case XmlTypeCode.NegativeInteger:
                    return new IntegerAttributeParser();
                case XmlTypeCode.Date:
                case XmlTypeCode.DateTime:
                    return new DateTimeAttributeParser();
            }

            throw new NotImplementedException($"Type not implemented: {typeCode} for attribute");
        }

        private IFlowtideXmlParser TypeCodeToParser(XmlTypeCode typeCode)
        {
            switch (typeCode)
            {
                case XmlTypeCode.String:
                case XmlTypeCode.Text:
                    return new StringXmlParser();
                case XmlTypeCode.UnsignedByte:
                case XmlTypeCode.UnsignedInt:
                case XmlTypeCode.UnsignedLong:
                case XmlTypeCode.UnsignedShort:
                case XmlTypeCode.Byte:
                case XmlTypeCode.Int:
                case XmlTypeCode.Integer:
                case XmlTypeCode.Long:
                case XmlTypeCode.Short:
                case XmlTypeCode.PositiveInteger:
                case XmlTypeCode.NonNegativeInteger:
                case XmlTypeCode.NegativeInteger:
                    return new IntegerElementParser();
                case XmlTypeCode.Date:
                case XmlTypeCode.DateTime:
                    return new DateTimeElementParser();
            }
            throw new NotImplementedException($"Type not implemented: {typeCode} for element");
        }
    }
}
