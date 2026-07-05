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

using FlowtideDotNet.Orleans.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Orleans.Interfaces
{
    public interface IStreamGrain : IGrainWithStringKey
    {
        Task StartStreamAsync(StartStreamRequest request);

        /// <summary>
        /// Stops all substreams of the stream together. Stopping them together lets the
        /// coordinated stop drain the data exchanged between the substreams, so everything
        /// sent before the stop is part of the final checkpoints. The grain persists which
        /// substreams it started, so the caller does not need to supply anything.
        /// </summary>
        Task StopStreamAsync();
    }
}
