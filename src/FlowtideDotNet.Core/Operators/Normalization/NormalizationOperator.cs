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

using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Core.Utils;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Normalization
{
    /// <summary>
    /// Takes input based on a key and keeps the latest value and sends deletes on the previous value.
    /// </summary>
    internal class NormalizationOperator : UnaryVertex<StreamEventBatch>
    {
#if DEBUG_WRITE
        private StreamWriter? allOutput;
#endif
        private readonly NormalizationRelation normalizationRelation;
        private IBPlusTree<string, IngressData, ListKeyContainer<string>, ListValueContainer<IngressData>>? _tree;
        private readonly Func<RowEvent, bool>? _filter;

        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        public override string DisplayName => "Normalize";

        public NormalizationOperator(
            NormalizationRelation normalizationRelation,
            FunctionsRegister functionsRegister,
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.normalizationRelation = normalizationRelation;
            if (normalizationRelation.Filter != null)
            {
                _filter = BooleanCompiler.Compile<RowEvent>(normalizationRelation.Filter, functionsRegister);
            }
        }

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override async Task OnCheckpoint()
        {
#if DEBUG_WRITE
            allOutput!.WriteLine("Checkpoint");
            await allOutput!.FlushAsync();
#endif
            Debug.Assert(_tree != null, nameof(_tree));
            // Commit changes in the tree
            await _tree.Commit();
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            _eventsProcessed.Add(msg.Events.Count);

            List<RowEvent> output = new List<RowEvent>();
            foreach (var e in msg.Events)
            {
                if (e.Weight > 0)
                {
                    var stringBuilder = new StringBuilder();
                    foreach (var i in normalizationRelation.KeyIndex)
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
                allOutput!.WriteLine($"{e.Weight} {e.ToJson()}");
            }
            await allOutput!.FlushAsync();
#endif
            if (output.Count > 0)
            {
                Debug.Assert(_eventsCounter != null, nameof(_eventsCounter));
                _eventsCounter.Add(output.Count);
                yield return new StreamEventBatch(output, normalizationRelation.OutputLength);
            }

        }

        protected async Task Upsert(string ke, RowEvent input, List<RowEvent> output)
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

        private async Task Upsert_Internal(string ke, RowEvent input, List<RowEvent> output)
        {
            Debug.Assert(_tree != null, nameof(_tree));

            bool isUpdate = false;
            byte[]? previousValue = null;

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
                if (previousValue == null)
                {
                    throw new InvalidOperationException("Previous value was null, should not happen");
                }
                output.Add(new RowEvent(1, 0, new CompactRowData(ingressInput.Memory)));
                output.Add(new RowEvent(-1, 0, new CompactRowData(previousValue)));
            }
            else if (added)
            {
                output.Add(new RowEvent(1, 0, new CompactRowData(ingressInput.Memory)));
            }
        }

        protected async Task Delete(string ke, List<RowEvent> output)
        {
            Debug.Assert(_tree != null);
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
                output.Add(new RowEvent(-1, 0, new CompactRowData(val.Memory)));
            }
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
#if DEBUG_WRITE
            if (!Directory.Exists("debugwrite"))
            {
                Directory.CreateDirectory("debugwrite");
            }
            if (allOutput == null)
            {
                allOutput = File.CreateText($"debugwrite/{StreamName}_{Name}.alloutput.txt");
            }
            else
            {
                allOutput.WriteLine("Restart");
                allOutput.Flush();
            }
#endif
            Logger.InitializingNormalizationOperator(StreamName, Name);
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            _tree = await stateManagerClient.GetOrCreateTree("input",
                new BPlusTreeOptions<string, IngressData, ListKeyContainer<string>, ListValueContainer<IngressData>>()
                {
                    Comparer = new BPlusTreeListComparer<string>(StringComparer.Ordinal),
                    KeySerializer = new KeyListSerializer<string>(new StringSerializer()),
                    ValueSerializer = new ValueListSerializer<IngressData>(new IngressDataStateSerializer()),
                    MemoryAllocator = MemoryAllocator
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
