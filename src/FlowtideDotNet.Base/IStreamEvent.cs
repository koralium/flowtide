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

namespace FlowtideDotNet.Base
{
    /// <summary>
    /// The base interface for all events that can flow through a stream.
    /// </summary>
    /// <remarks>
    /// This interface is implemented by different types of stream events, such as data events representing 
    /// rows or batches of data, watermarks that track logical time, and locking events used for checkpoints.
    /// </remarks>
    public interface IStreamEvent
    {
    }
}
