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

using FlowtideDotNet.Base.Engine.Internal.StateMachine;

namespace FlowtideDotNet.Base.Engine
{
    public ref struct StreamStateChangeNotification
    {
        public readonly ref string StreamName;
        public readonly ref StreamStateValue State;
        public StreamStateChangeNotification(ref string streamName, ref StreamStateValue state)
        {
            StreamName = ref streamName;
            State = ref state;
        }
    }

    public ref struct StreamCheckpointNotification
    {
        public readonly ref string StreamName;
        public StreamCheckpointNotification(ref string streamName)
        {
            StreamName = ref streamName;
        }
    }

    public ref struct StreamFailureNotification
    {
        public readonly ref string StreamName;
        public readonly Exception? Exception;
        public StreamFailureNotification(ref string streamName, Exception? exception)
        {
            StreamName = ref streamName;
            Exception = exception;
        }
    }
}
