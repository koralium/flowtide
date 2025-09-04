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

using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Debug;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests
{
    /// <summary>
    /// Base class for testing operators
    /// </summary>
    public abstract class OperatorTestBase
    {
        private StateManagerSync<StreamState>? _stateManager;
        internal FunctionsRegister FunctionsRegister { get; private set; }

        public OperatorTestBase()
        {
            FunctionsRegister = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(FunctionsRegister);
        }

        public async Task InitializeOperator(IStreamVertex @operator)
        {
            @operator.Setup("stream", "1");
            @operator.CreateBlock();
            @operator.Link();
            var metrics = new StreamMetrics("stream");
            var statemanagermeter = new Meter("statemanager");
            _stateManager = new StateManagerSync<StreamState>(new StateManagerOptions(), new DebugLoggerProvider().CreateLogger("state"), statemanagermeter, "stream");
            await _stateManager.InitializeAsync();
            var vertexHandler = new VertexHandler(
                "stream", 
                "1", 
                (time, version) => { }, 
                (p1, p2, t) => Task.CompletedTask, 
                metrics.GetOrCreateVertexMeter("1", () => ""), 
                _stateManager.GetOrCreateClient("1"), 
                new LoggerFactory(), 
                new OperatorMemoryManager("stream", "op", new Meter("stream")),
                (exception, version) => Task.CompletedTask);
            await @operator.Initialize("1", 0, 0, vertexHandler, null);
        }

        public void ClearCache()
        {
            if (_stateManager != null)
            {
                _stateManager.ClearCache();
            }
        }
    }
}
