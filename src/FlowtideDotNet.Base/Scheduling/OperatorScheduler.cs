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

using FlowtideDotNet.Base.Vertices;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Scheduling
{
    internal class OperatorScheduler : TaskScheduler
    {
        //OperatorWorkGroup group;
        private readonly StreamScheduler streamScheduler;

        public int OperatorId { get; set; } = -1;

        public IStreamVertex? Vertex { get; set; }

        public float Weight { get; set; } = 1.0f;

        private float busyEma = 0;

        private OperatorTask? latestTask;
        private Type _sourceCoreType;
        private Type _targetCoreType;

        public OperatorScheduler(StreamScheduler streamScheduler)
        {
            //group = new OperatorWorkGroup(this, streamScheduler);
            this.streamScheduler = streamScheduler;
            var sourceCoreType = typeof(BufferBlock<>).Assembly.GetType("System.Threading.Tasks.Dataflow.Internal.SourceCore`1");
            if (sourceCoreType == null)
            {
                throw new InvalidOperationException("Could not find SourceCore type in Dataflow assembly.");
            }
            _sourceCoreType = sourceCoreType.MakeGenericType(typeof(IStreamEvent));
            var targetCoreType = typeof(BufferBlock<>).Assembly.GetType("System.Threading.Tasks.Dataflow.Internal.TargetCore`1");
            if (targetCoreType == null)
            {
                throw new InvalidOperationException("Could not find TargetCore type in Dataflow assembly.");
            }
            _targetCoreType = targetCoreType.MakeGenericType(typeof(IStreamEvent));
        }

        protected override IEnumerable<Task>? GetScheduledTasks()
        {
            throw new NotImplementedException();
        }

        protected override void QueueTask(Task task)
        {
            if (Vertex != null) 
            {
                var busy = Vertex.Busy * Weight;
                var operatorTask = new OperatorTask(this, task);
                if (latestTask != null && busy >= 0.9f)
                {
                    // If the operator is busy, chain together tasks to run on the same thread
                    // to utilize the same caching
                    latestTask._nextTask = operatorTask;
                }
                latestTask = operatorTask;
                streamScheduler.QueueTask(operatorTask, busy);
            }
            else
            {
                streamScheduler.QueueTask(new OperatorTask(this, task));
            }
        }

        public bool ExecuteTask(Task task)
        {
            return TryExecuteTask(task);
        }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            return this.streamScheduler.TryExecuteTaskInline(task, this, taskWasPreviouslyQueued);
        }
    }
}
