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

using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Substrait.Relations;
using System.Text;
using System.Threading.Tasks.Dataflow;
using System.Diagnostics.Metrics;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Serializers;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute;

namespace FlowtideDotNet.Core.Operators.Normalization
{
    /// <summary>
    /// Takes input based on a key and keeps the latest value and sends deletes on the previous value.
    /// </summary>
    internal class NormalizationOperator : UnaryVertex<StreamEventBatch, NormalizationState>
    {
#if DEBUG_WRITE
        private StreamWriter allOutput;
#endif
        private readonly NormalizationRelation normalizationRelation;
        private IBPlusTree<string, IngressData>? _tree;
        private readonly Func<StreamEvent, bool>? _filter;

        private Counter<long>? _eventsCounter;
        
        public override string DisplayName => "Normalize";

        public NormalizationOperator(
            NormalizationRelation normalizationRelation, 
            FunctionsRegister functionsRegister,
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.normalizationRelation = normalizationRelation;
            if (normalizationRelation.Filter != null)
            {
                _filter = BooleanCompiler.Compile<StreamEvent>(normalizationRelation.Filter, functionsRegister);
            }
        }

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override async Task<NormalizationState> OnCheckpoint()
        {
#if DEBUG_WRITE
            allOutput.WriteLine("Checkpoint");
            await allOutput.FlushAsync();
#endif
            Debug.Assert(_tree != null, nameof(_tree));
            // Commit changes in the tree
            await _tree.Commit();
            return new NormalizationState();
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            List<StreamEvent> output = new List<StreamEvent>();
            foreach(var e in msg.Events)
            {
                if (e.Weight > 0)
                {
                    var stringBuilder = new StringBuilder();
                    foreach(var i in normalizationRelation.KeyIndex)
                    {
                        stringBuilder.Append(e.GetColumn(i).ToJson);
                        stringBuilder.Append('|');
                    }
                    var key = stringBuilder.ToString();
                    await Upsert(key, e, output);
                }
                else if (e.Weight < 0)
                {
                    var stringBuilder = new StringBuilder();
                    foreach (var i in normalizationRelation.KeyIndex)
                    {
                        stringBuilder.Append(e.GetColumn(i).ToJson);
                        stringBuilder.Append('|');
                    }
                    var key = stringBuilder.ToString();
                    await Delete(key, output);
                }
            }
            

#if DEBUG_WRITE
            foreach(var e in output)
            {
                allOutput.WriteLine($"{e.Weight} {e.Vector.ToJson}");
            }
            await allOutput.FlushAsync();
#endif
            if (output.Count > 0)
            {
                Debug.Assert(_eventsCounter != null, nameof(_eventsCounter));
                _eventsCounter.Add(output.Count);
                yield return new StreamEventBatch(null, output);
            }
            
        }

        protected async Task Upsert(string ke, StreamEvent input, List<StreamEvent> output)
        {
            // Filter is applied on the actual input
            if (_filter != null)
            {
                // Check that the input matches the filtr
                if (_filter(input))
                {
                    // Upsert the data
                    await Upsert_Internal(ke, input, output);
                }
                else
                {
                    // Test delete the data if it exists
                    await Delete(ke, output);
                }
            }
            else
            {
                await Upsert_Internal(ke, input, output);
            }
        }

        private async Task Upsert_Internal(string ke, StreamEvent input, List<StreamEvent> output)
        {
            Debug.Assert(_tree != null, nameof(_tree));

            bool isUpdate = false;
            Memory<byte>? previousValue = null;

            var ingressInput = IngressData.Create(b =>
            {
                if (normalizationRelation.EmitSet)
                {
                    for (int i = 0; i < normalizationRelation.Emit!.Count; i++)
                    {
                        var index = normalizationRelation.Emit[i];
                        b.Add(input.GetColumn(index));
                    }
                }
                else
                {
                    for (int i = 0; i < input.Length; i++)
                    {
                        b.Add(input.GetColumn(i));
                    }
                }
            });

            bool added = false;
            await _tree.RMW(ke, ingressInput, (input, current, found) =>
            {
                if (found)
                {
                    if (!current.Span.SequenceEqual(input.Span))
                    {
                        isUpdate = true;
                        // Save the previous value
                        previousValue = current.Memory;
                        // Replace with updated value
                        current.Memory = ingressInput.Memory;
                        return (current, GenericWriteOperation.Upsert);
                    }
                    return (current, GenericWriteOperation.None);
                }
                else
                {
                    isUpdate = false;
                    added = true;
                    return (input, GenericWriteOperation.Upsert);
                }
            });

            if (isUpdate)
            {
                if (!previousValue.HasValue)
                {
                    throw new InvalidOperationException("Previous value was null, should not happen");
                }
                output.Add(new StreamEvent(1, 0, ingressInput.Memory));
                output.Add(new StreamEvent(-1, 0, previousValue.Value));
            }
            else if (added)
            {
                output.Add(new StreamEvent(1, 0, ingressInput.Memory));
            }
        }

        protected async Task Delete(string ke, List<StreamEvent> output)
        {
            bool isFound = false;
            IngressData? data;
            
            var (op, val) = await _tree.RMW(ke, default, (input, current, found) =>
            {
                if (found)
                {
                    current.IsDeleted = true;
                    isFound = found;
                    data = current;
                    return (current, GenericWriteOperation.Delete);
                }
                return (default, GenericWriteOperation.None);
            });

            if (isFound)
            {
                output.Add(new StreamEvent(-1, 0, val.Memory));
            }
        }

        protected override async Task InitializeOrRestore(NormalizationState? state, IStateManagerClient stateManagerClient)
        {
#if DEBUG_WRITE
            allOutput = File.CreateText($"{Name}.alloutput.txt");
#endif
            Logger.LogInformation("Initializing normalization operator.");
            _eventsCounter = Metrics.CreateCounter<long>("events");
            _tree = await stateManagerClient.GetOrCreateTree("input", new BPlusTreeOptions<string, IngressData>()
            {
                Comparer = StringComparer.Ordinal,
                KeySerializer = new StringSerializer(),
                ValueSerializer = new IngressDataStateSerializer()
            });
        }

        public override ValueTask DisposeAsync()
        {
            // No operation required
            return base.DisposeAsync();
        }

        public override Task DeleteAsync()
        {
            // No operation required
            return Task.CompletedTask;
        }
    }
}
