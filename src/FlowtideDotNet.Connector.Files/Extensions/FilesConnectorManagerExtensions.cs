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

using FlowtideDotNet.Connector.Files;
using FlowtideDotNet.Connector.Files.Internal;
using FlowtideDotNet.Connector.Files.Internal.CsvFiles;
using FlowtideDotNet.Connector.Files.Internal.TextLineFiles;
using FlowtideDotNet.Connector.Files.Internal.XmlFiles;
using FlowtideDotNet.Core;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Schema;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class FilesConnectorManagerExtensions
    {
        public static IConnectorManager AddCsvFileSource(this IConnectorManager connectorManager, string tableName, CsvFileOptions options)
        {
            var deltaColumns = options.DeltaCsvColumns;

            if (deltaColumns == null)
            {
                deltaColumns = options.CsvColumns;
            }

            var deltaWeightFunction = options.DeltaWeightFunction;
            if (deltaWeightFunction == null)
            {
                deltaWeightFunction = (values) => 1;
            }

            var initialWeightFunction = options.InitialWeightFunction;
            if (initialWeightFunction == null)
            {
                initialWeightFunction = (values) => 1;
            }

            var outputSchema = options.OutputSchema;
            if (outputSchema == null)
            {
                List<string> columnNames = [.. options.CsvColumns];
                outputSchema = new NamedStruct()
                {
                    Names = columnNames,
                    Struct = new Struct()
                    {
                        Types = columnNames.Select(x => (SubstraitBaseType)new StringType()).ToList()
                    }
                };
            }

            var internalOptions = new CsvFileInternalOptions()
            {
                CsvColumns = options.CsvColumns,
                DeltaCsvColumns = deltaColumns,
                DeltaWeightFunction = deltaWeightFunction,
                FileStorage = options.FileStorage,
                GetInitialFiles = options.GetInitialFiles,
                InitialWeightFunction = initialWeightFunction,
                OutputSchema = outputSchema,
                BeforeReadFile = options.BeforeReadFile,
                DeltaGetNextFile = options.DeltaGetNextFile,
                DeltaInterval = options.DeltaInterval,
                FilesHaveHeader = options.FilesHaveHeader,
                ModifyRow = options.ModifyRow,
                Delimiter = options.Delimiter,
                BeforeBatch = options.BeforeBatch
            };

            connectorManager.AddSource(new CsvFileDataSourceFactory(tableName, internalOptions));
            return connectorManager;
        }

        public static IConnectorManager AddXmlFileSource(this IConnectorManager connectorManager, string tableName, XmlFileOptions xmlFileOptions)
        {
            var schemaReader = new StringReader(xmlFileOptions.XmlSchema);
            var schemaSet = new XmlSchemaSet();

            var schema = XmlSchema.Read(schemaReader, (obj, events) =>
            {

            });

            if (schema == null)
            {
                throw new InvalidOperationException($"Failed to read XML schema from {xmlFileOptions.XmlSchema}");
            }

            schemaSet.Add(schema);

            var baseType = new XmlTypeParser().ParseElement(xmlFileOptions.ElementName, schemaSet);

            if (baseType is not NamedStruct namedStruct)
            {
                throw new InvalidOperationException($"Element {xmlFileOptions.ElementName} is not a complex type.");
            }

            if (xmlFileOptions.ExtraColumns != null)
            {
                foreach (var extraColumn in xmlFileOptions.ExtraColumns)
                {
                    namedStruct.Names.Add(extraColumn.ColumnName);
                    if (namedStruct.Struct != null)
                    {
                        namedStruct.Struct.Types.Add(extraColumn.DataType);
                    }
                }
            }

            connectorManager.AddSource(new XmlFileDataSourceFactory(tableName, new XmlFileInternalOptions()
            {
                ElementName = xmlFileOptions.ElementName,
                FileStorage = xmlFileOptions.FileStorage,
                FlowtideSchema = namedStruct,
                GetInitialFiles = xmlFileOptions.GetInitialFiles,
                XmlSchema = schemaSet,
                BeforeBatch = xmlFileOptions.BeforeBatch,
                DeltaGetNextFiles = xmlFileOptions.DeltaGetNextFiles,
                DeltaInterval = xmlFileOptions.DeltaInterval,
                ExtraColumns = xmlFileOptions.ExtraColumns ?? new List<FileExtraColumn>(),
            }));

            return connectorManager;
        }

        public static IConnectorManager AddTextLinesFileSource(this IConnectorManager connectorManager, string tableName, TextLinesFileOptions options)
        {
            var internalOptions = new TextLineInternalOptions()
            {
                FileStorage = options.FileStorage,
                GetInitialFiles = options.GetInitialFiles,
                BeforeBatch = options.BeforeBatch,
                DeltaGetNextFiles = options.DeltaGetNextFiles,
                DeltaInterval = options.DeltaInterval,
                ExtraColumns = options.ExtraColumns ?? new List<FileExtraColumn>()
            };

            connectorManager.AddSource(new TextLineFileDataSourceFactory(tableName, internalOptions));
            return connectorManager;
        }
    }
}
