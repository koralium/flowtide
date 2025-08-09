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

using System.Xml.Schema;
using System.Xml;
using FlowtideDotNet.Substrait.Type;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.Files.Internal.XmlFiles
{
    /// <summary>
    /// Parses an XML schema and converts it to Substrait types.
    /// This is used to create the named struct used for the sql parser.
    /// </summary>
    internal class XmlTypeParser
    {
        private Dictionary<XmlQualifiedName, XmlSchemaType>? schemaTypes;
        private Dictionary<XmlQualifiedName, XmlSchemaElement>? globalElements;

        public SubstraitBaseType ParseElement(string elementName, XmlSchemaSet schemaSet)
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
            foreach(var kv in globalElements)
            {
                var result = TryFindElement(elementName, kv.Value);
                if (result != null)
                {
                    return result;
                }
            }
            throw new Exception($"Element not found: {elementName}");
        }

        private SubstraitBaseType? TryFindElement(string name, XmlSchemaElement element)
        {
            if (name == element.Name)
                return ParseElementInternal(element);
            XmlSchemaType? schemaType = element.ElementSchemaType;
            if (schemaType is XmlSchemaComplexType complexType)
            {
                if (complexType.ContentTypeParticle is XmlSchemaSequence sequence)
                {
                    foreach(var item in sequence.Items)
                    {
                        if (item is XmlSchemaElement childElement)
                        {
                            var result = TryFindElement(name, childElement);
                            if (result != null)
                            {
                                return result;
                            }
                        }
                            
                    }
                }
            }
            return default;
        }

        private SubstraitBaseType ParseElementInternal(XmlSchemaElement element)
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
                return TypeCodeToType(simpleType.TypeCode);
            }

            if (element.SchemaTypeName != null && !element.SchemaTypeName.IsEmpty)
            {
                if (element.SchemaTypeName.Namespace == "http://www.w3.org/2001/XMLSchema")
                {
                    return new AnyType();
                }

                if (schemaTypes.TryGetValue(element.SchemaTypeName, out var namedType))
                    schemaType = namedType;
                else
                    throw new Exception($"Type not found: {element.SchemaTypeName}");
            }

            if (schemaType is XmlSchemaComplexType complexType)
            {
                List<string> names = new List<string>();
                List<SubstraitBaseType> types = new List<SubstraitBaseType>();

                if (complexType.ContentTypeParticle is XmlSchemaSequence sequence)
                {
                    foreach (XmlSchemaObject item in sequence.Items)
                    {
                        if (item is XmlSchemaElement childElement)
                        {
                            // Check if this is a list or not
                            var type = ParseElementInternal(childElement);
                            if (childElement.MaxOccurs > 1)
                            {
                                type = new ListType(type);
                            }
                            names.Add(childElement.Name!);
                            types.Add(type);
                        }
                    }
                }
                else if (complexType.ContentTypeParticle is XmlSchemaAll allSchema)
                {
                    foreach (var item in allSchema.Items)
                    {
                        if (item is XmlSchemaElement childElement)
                        {
                            var type = ParseElementInternal(childElement);
                            if (childElement.MaxOccurs > 1)
                            {
                                type = new ListType(type);
                            }
                            names.Add(childElement.Name!);
                            types.Add(type);
                        }
                    }
                }

                // Handle attributes
                foreach (XmlSchemaObject attrObj in complexType.Attributes)
                {
                    if (attrObj is XmlSchemaAttribute attribute)
                    {
                        names.Add(attribute.Name!);

                        SubstraitBaseType type = AnyType.Instance;

                        if (attribute.AttributeSchemaType != null)
                        {
                            type = TypeCodeToType(attribute.AttributeSchemaType.TypeCode);
                        }

                        types.Add(type);
                    }
                }


                return new NamedStruct()
                {
                    Names = names,
                    Struct = new Struct()
                    {
                        Types = types
                    }
                };
            }

            throw new NotImplementedException($"Type not implemented: {schemaType!.GetType()}");
        }

        private SubstraitBaseType TypeCodeToType(XmlTypeCode typeCode)
        {
            switch (typeCode)
            {
                case XmlTypeCode.Boolean:
                    return new BoolType();
                case XmlTypeCode.Decimal:
                    return new DecimalType();
                case XmlTypeCode.Double:
                case XmlTypeCode.Float:
                    return new Fp64Type();
                case XmlTypeCode.String:
                case XmlTypeCode.Text:
                    return new StringType();
                case XmlTypeCode.Short:
                case XmlTypeCode.Int:
                case XmlTypeCode.Integer:
                case XmlTypeCode.NegativeInteger:
                case XmlTypeCode.NonNegativeInteger:
                case XmlTypeCode.UnsignedByte:
                case XmlTypeCode.UnsignedInt:
                case XmlTypeCode.UnsignedLong:
                    return new Int64Type();
                case XmlTypeCode.Date:
                case XmlTypeCode.DateTime:
                    return new TimestampType();
            }
            return new AnyType();
        }
    }
}
