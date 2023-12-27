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

using ConsoleTables;
using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Sinks
{
    internal class ConsoleSink : EgressVertex<StreamEventBatch, object>
    {
        private readonly WriteRelation writeRelation;

        public ConsoleSink(WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.writeRelation = writeRelation;
        }

        public override string DisplayName => "Console";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        protected override Task InitializeOrRestore(long restoreTime, object? state, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override Task<object> OnCheckpoint(long checkpointTime)
        {
            return Task.FromResult(new object());
        }

        protected override Task OnRecieve(StreamEventBatch msg, long time)
        {
            var consoleTable = new ConsoleTable(writeRelation.TableSchema.Names.Prepend("Weight").ToArray());
            foreach (var e in msg.Events)
            {
                var vals = new object[writeRelation.TableSchema.Names.Count + 1];
                vals[0] = e.Weight;

                for (int i = 0; i < e.Length; i++)
                {
                    vals[i + 1] = e.GetColumn(i).ToJson;
                }

                consoleTable.AddRow(vals);
                //if (e.Weight > 0)
                //{
                //    Console.WriteLine($"w: +{e.Weight}, data: {e.Vector.ToJson}");
                //}
                //else
                //{
                //    Console.WriteLine($"w: -{e.Weight}, data: {e.Vector.ToJson}");
                //}
            }
            consoleTable.Write(Format.Default);
            return Task.CompletedTask;
        }
    }
}
