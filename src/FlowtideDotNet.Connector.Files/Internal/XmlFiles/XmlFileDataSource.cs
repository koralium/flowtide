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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Connector.Files.Internal.XmlFiles.XmlParsers;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Xml;

namespace FlowtideDotNet.Connector.Files.Internal.XmlFiles
{
    internal class XmlFileDataSource : ReadBaseOperator
    {
        private readonly XmlFileInternalOptions _fileOptions;
        private readonly ReadRelation _readRelation;
        private IFlowtideXmlParser _parser;
        private readonly string _watermarkName;

        public XmlFileDataSource(XmlFileInternalOptions fileOptions, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            _fileOptions = fileOptions;
            this._readRelation = readRelation;
            _watermarkName = readRelation.NamedTable.DotSeperated;

            _parser = new SchemaToParsers().ParseElement(fileOptions.ElementName, fileOptions.XmlSchema);
        }

        public override string DisplayName => $"XmlFile({_watermarkName})";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _watermarkName });
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            return Task.CompletedTask;
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            using var stream = await _fileOptions.FileStorage.OpenRead(_fileOptions.InitialFile);

            if (stream == null)
            {
                throw new InvalidOperationException($"File {_fileOptions.InitialFile} not found");
            }

            var reader = XmlReader.Create(stream);

            while (await reader.ReadAsync())
            {
                if (reader.Name.Equals(_fileOptions.ElementName, StringComparison.OrdinalIgnoreCase))
                {
                    var val = _parser.Parse(reader);
                }
            }
        }
    }
}
