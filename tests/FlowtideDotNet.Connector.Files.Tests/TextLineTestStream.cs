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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Files.Tests
{
    internal class TextLineTestStream : FlowtideTestStream
    {
        private readonly TextLinesFileOptions options;

        public TextLineTestStream(string testName, TextLinesFileOptions options) : base(testName)
        {
            this.options = options;
        }

        protected override void AddReadResolvers(IConnectorManager connectorManger)
        {
            connectorManger.AddTextLinesFileSource("test", options);
        }
    }
}
