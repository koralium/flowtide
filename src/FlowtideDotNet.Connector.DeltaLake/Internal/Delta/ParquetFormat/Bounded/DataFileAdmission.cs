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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using ParquetSharp;
using System.Diagnostics;
using DeltaArrayType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.ArrayType;
using DeltaDecimalType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.DecimalType;
using DeltaMapType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.MapType;
using DeltaStructType = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types.StructType;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded
{
    // An output leaf as the writer terms see it
    // Encoded is a dictionary entry's Parquet bytes
    internal readonly record struct OutputLeaf(LeafShape Shape, int Width, bool Repeated, int Encoded, bool Decimal);

    // One leaf's load: bytes, levels, levels per row and the largest value
    // Typical row levels and value; the flush recheck measures the largest
    internal readonly record struct LeafLoad(long Bytes, long Levels, bool Repeated, long RowLevels, long Value, LeafShape Shape, int Encoded, bool Decimal);

    // The settings one output fixes for every input of its bin
    internal sealed record AdmissionSettings(long FlushBytes, Compression Compression)
    {
        public long NativeCap { get; init; } = JobMemory.DefaultNativeCap;

        public long ManagedCap { get; init; } = JobMemory.DefaultManagedCap;

        public long TransportChunk { get; init; } = StowageCheckpointTransport.DefaultChunkSize;
    }

    // The table's output nodes: buffers, leaves and the bits every row costs
    internal sealed class OutputShape
    {
        public OutputShape(DeltaStructType table)
        {
            var leaves = new List<OutputLeaf>();
            long buffers = 0, rowBits = 0;
            foreach (var field in table.Fields)
            {
                Walk(field.Type, false, leaves, ref buffers, ref rowBits);
            }
            Leaves = leaves;
            Buffers = buffers;
            RowBits = Math.Max(1, rowBits);
        }

        public IReadOnlyList<OutputLeaf> Leaves { get; }

        // K_out, as BuilderSizes counts them
        public long Buffers { get; }

        // Bits one row adds outside lists and maps
        public long RowBits { get; }

        private static void Walk(SchemaBaseType type, bool repeated, List<OutputLeaf> leaves, ref long buffers, ref long rowBits)
        {
            switch (type)
            {
                case DeltaStructType structType:
                    buffers++;
                    rowBits += repeated ? 0 : 1;
                    foreach (var field in structType.Fields)
                    {
                        Walk(field.Type, repeated, leaves, ref buffers, ref rowBits);
                    }
                    break;
                case DeltaArrayType arrayType:
                    buffers += 2;
                    rowBits += repeated ? 0 : 33;
                    Walk(arrayType.ElementType!, true, leaves, ref buffers, ref rowBits);
                    break;
                case DeltaMapType mapType:
                    buffers += 2;
                    rowBits += repeated ? 0 : 33;
                    Walk(mapType.KeyType, true, leaves, ref buffers, ref rowBits);
                    Walk(mapType.ValueType, true, leaves, ref buffers, ref rowBits);
                    break;
                default:
                    var (shape, width) = Shape(type);
                    buffers += shape == LeafShape.Bytes ? 3 : 2;
                    rowBits += repeated ? 0 : shape switch { LeafShape.Fixed => 1 + 8 * width, LeafShape.Bits => 2, _ => 33 };
                    // Ints widen to INT32, a decimal is the FLBA its precision needs
                    var encoded = type is DeltaDecimalType decimalType
                        ? (int)Math.Ceiling((decimalType.Precision * Math.Log2(10) + 1) / 8)
                        : type.Type is SchemaType.Long or SchemaType.Double or SchemaType.Timestamp ? 8 : 4;
                    leaves.Add(new OutputLeaf(shape, width, repeated, encoded, type is DeltaDecimalType));
                    break;
            }
        }

        public static (LeafShape Shape, int Width) Shape(SchemaBaseType type) => type.Type switch
        {
            SchemaType.Boolean => (LeafShape.Bits, 0),
            SchemaType.Byte => (LeafShape.Fixed, 1),
            SchemaType.Short => (LeafShape.Fixed, 2),
            SchemaType.Integer or SchemaType.Float or SchemaType.Date => (LeafShape.Fixed, 4),
            SchemaType.Long or SchemaType.Double or SchemaType.Timestamp => (LeafShape.Fixed, 8),
            SchemaType.Decimal => (LeafShape.Fixed, 16),
            _ => (LeafShape.Bytes, 0)
        };
    }

    // One admitted input's terms, kept until it is copied or dropped
    internal sealed class InputSummary
    {
        // B = 65,536 down to 1, halving
        public const int Candidates = 17;

        public static long BatchRows(int candidate) => 65_536L >> candidate;

        public required long Metadata { get; init; }

        public required long Rows { get; init; }

        // Model_i, the scan's managed charge
        public required long Model { get; init; }

        // out_i and out_iℓ, the output batch of every group whole
        public required long Out { get; init; }

        // Per output leaf, the largest share of a group's unrounded bytes
        public required double[] LeafShare { get; init; }

        // The largest value per output leaf, for byte-array statistics
        public required long[] LeafMaxValue { get; init; }

        // Per output leaf, a group's largest mean value
        public required long[] LeafMeanValue { get; init; }

        // Per output leaf, the most levels per unrounded byte
        public required double[] LeafLevelRatio { get; init; }

        // Per output leaf, the largest mean levels per row of a group
        public required long[] LeafRowLevels { get; init; }

        // Per output leaf, a group's most level slots
        public required long[] LeafGroupLevels { get; init; }

        public required int Groups { get; init; }

        // Per candidate, maxima over groups
        public required long[] ReadPeak { get; init; }

        public required long[] ReaderOnly { get; init; }

        public required long[] OutBatch { get; init; }

        public required long[] SynthStage { get; init; }

        public required long PrecisionDecode { get; init; }

        public required long PrecisionManaged { get; init; }

        // At B = 1, the group with the largest read peak
        public required (long Dictionary, long Decode, long ReadRepeated, long ReadFlat, long Synth, long SynthOut, long RepeatedOut) Floor { get; init; }

        // Chosen by admission
        public int Candidate { get; set; } = -1;

        // Synth_g and Out_g at the chosen B, for the copier
        public long[] GroupSynth { get; set; } = Array.Empty<long>();

        public long[] GroupOut { get; set; } = Array.Empty<long>();

        // Per group, the most level slots of any list leaf
        public long[] GroupLevels { get; set; } = Array.Empty<long>();

        // Out_g of each whole group, for the pre-flush
        public long[] GroupWhole { get; set; } = Array.Empty<long>();

        // This summary's own managed bytes, the group checks counted from admission
        public long Bytes => 512 + 8L * (LeafShare.Length * 6 + Candidates * 4 + 4 * Groups);
    }

    // One input's pass-2 reservations: native peaks, charged groups, managed parts
    internal readonly record struct InputTerms(long NativeCopy, long NativePrecision, long Groups, long Statistics, long Transient, long DeletionVector, long Managed);

    // The writer terms over a frozen prefix
    internal readonly record struct SharedTerms(long Cap, long Built, long Writer);

    // Admission's verdict for one input: its batch candidate, or a decline
    // A prefix drop is the bin's, not the input's
    internal readonly record struct Admission(int Candidate, CompactionReason? Reason, string Detail, bool PrefixDrop = false);

    // The memory admission of §4.3.7, from footers and page headers alone
    internal static class DataFileAdmission
    {
        // Measured in M3's audit, see native-audit.md
        public const long StatisticsPerLeaf = 6L << 10;
        public const long PrecisionCursor = 4096 * 16 + (16L << 10);

        // The footer, binding and page terms, or a decline
        public static (DataFileScan Scan, BindingPlan Plan) Preflight(string path, DeltaStructType table, bool byId, JobMemory memory, ScanLimits? limits = null)
        {
            BindingPlan? plan = null;
            var scan = DataPageHeaderScan.Scan(path, memory, schema =>
            {
                plan = BindingPreflight.Plan(table, schema, byId);
                return plan.Read;
            }, limits);
            return (scan, plan!);
        }

        public static InputSummary Summarize(DataFileScan scan, BindingPlan plan, OutputShape output)
        {
            try
            {
                return Summary(scan, plan, output);
            }
            catch (OverflowException e)
            {
                throw new DataFileDeclinedException(CompactionReason.BoundOverflow, e.Message);
            }
        }

        private static InputSummary Summary(DataFileScan scan, BindingPlan plan, OutputShape output)
        {
            checked
            {
                var groups = scan.Groups.Length;
                var totals = new OutputTotals(output.Leaves.Count);
                long[] readPeak = new long[InputSummary.Candidates], readerOnly = new long[InputSummary.Candidates], outBatch = new long[InputSummary.Candidates], stage = new long[InputSummary.Candidates];
                long total = 0, precisionDecode = 0, precisionManaged = 0;
                var floor = default((long, long, long, long, long, long, long));
                var precision = plan.Precision.Select(x => x.ColumnIndex).ToHashSet();
                for (int g = 0; g < groups; g++)
                {
                    var rows = scan.Footer.RowGroupRows[g];
                    var (decode, _) = scan.Decode(g);
                    var whole = new OutputTerms(scan, g, rows);
                    total += whole.Out(plan, totals);
                    totals.EndGroup();
                    for (int k = 0; k < InputSummary.Candidates; k++)
                    {
                        var b = InputSummary.BatchRows(k);
                        var (batch, recordReader, readRepeated) = scan.ReadBatch(g, b);
                        var terms = new OutputTerms(scan, g, Math.Min(b, rows));
                        var synth = terms.Synth(plan, out var largest);
                        var outBytes = terms.Out(plan, null);
                        readPeak[k] = Math.Max(readPeak[k], decode + recordReader + batch + synth);
                        readerOnly[k] = Math.Max(readerOnly[k], decode + recordReader);
                        outBatch[k] = Math.Max(outBatch[k], outBytes);
                        stage[k] = Math.Max(stage[k], largest);
                        if (k == InputSummary.Candidates - 1 && decode + recordReader + batch + synth >= floor.Item2 + floor.Item3 + floor.Item4 + floor.Item5)
                        {
                            var parts = new long[3];
                            terms.Out(plan, null, parts);
                            floor = (Dictionary(scan, g), decode, readRepeated, batch + recordReader - readRepeated, synth, parts[0], parts[1]);
                        }
                    }
                    if (precision.Count > 0)
                    {
                        precisionDecode = Math.Max(precisionDecode, scan.Decode(g, leaf => precision.Contains(leaf)).Decode);
                        precisionManaged = Math.Max(precisionManaged, PrecisionCursor + (rows + 7) / 8);
                    }
                }
                return new InputSummary()
                {
                    Metadata = scan.Metadata,
                    Rows = scan.Footer.NumRows,
                    Model = scan.ChargedBytes,
                    Out = total,
                    LeafShare = totals.Share,
                    LeafMaxValue = totals.MaxValue,
                    LeafMeanValue = totals.MeanValue,
                    LeafLevelRatio = totals.LevelRatio,
                    LeafRowLevels = totals.RowLevels,
                    LeafGroupLevels = totals.GroupLevels,
                    Groups = groups,
                    ReadPeak = readPeak,
                    ReaderOnly = readerOnly,
                    OutBatch = outBatch,
                    SynthStage = stage,
                    PrecisionDecode = precisionDecode,
                    PrecisionManaged = precisionManaged,
                    Floor = floor
                };
            }
        }

        // Per-group terms for the copier
        public static void KeepGroupChecks(InputSummary summary, DataFileScan scan, BindingPlan plan, OutputShape output)
        {
            var b = InputSummary.BatchRows(summary.Candidate);
            summary.GroupSynth = new long[scan.Groups.Length];
            summary.GroupOut = new long[scan.Groups.Length];
            summary.GroupLevels = new long[scan.Groups.Length];
            summary.GroupWhole = new long[scan.Groups.Length];
            for (int g = 0; g < scan.Groups.Length; g++)
            {
                var terms = new OutputTerms(scan, g, Math.Min(b, scan.Footer.RowGroupRows[g]));
                summary.GroupSynth[g] = terms.Synth(plan, out _);
                summary.GroupOut[g] = terms.Out(plan, null);
                var totals = new OutputTotals(output.Leaves.Count);
                summary.GroupWhole[g] = new OutputTerms(scan, g, scan.Footer.RowGroupRows[g]).Out(plan, totals);
                totals.EndGroup();
                summary.GroupLevels[g] = Enumerable.Range(0, output.Leaves.Count).Where(l => output.Leaves[l].Repeated).Select(l => totals.GroupLevels[l]).DefaultIfEmpty().Max();
            }
        }

        // Floor at B = 1, then the largest batch, then the prefix rechecked
        public static Admission Admit(IReadOnlyList<InputSummary> prefix, InputSummary input, OutputShape output, AdmissionSettings settings)
        {
            Debug.Assert(prefix.All(x => x.Candidate >= 0), "Every earlier input has its batch size");
            try
            {
                checked
                {
                    var last = InputSummary.Candidates - 1;
                    var floor = Check(prefix, input, last, output, settings);
                    if (floor.Native > settings.NativeCap || floor.Managed > settings.ManagedCap)
                    {
                        // A native miss names its largest term
                        var (reason, detail) = floor.Native > settings.NativeCap ? Dominant(prefix, input, floor, output, settings) : (CompactionReason.ManagedBound, "");
                        // Fitting alone, it fails only beside the prefix, which then freezes
                        var drop = prefix.Count > 0 && Admit(System.Array.Empty<InputSummary>(), input, output, settings).Reason == null;
                        return new Admission(-1, reason, $"At one row per batch the input needs {floor.Native} native bytes and {floor.Managed} managed{detail}", PrefixDrop: drop);
                    }
                    for (int k = 0; k < InputSummary.Candidates; k++)
                    {
                        var own = Check(prefix, input, k, output, settings);
                        if (own.Native > settings.NativeCap || own.Managed > settings.ManagedCap)
                        {
                            continue;
                        }
                        // The running prefix: halve until earlier inputs still fit
                        CompactionReason? miss = null;
                        for (var halved = k; halved < InputSummary.Candidates; halved++)
                        {
                            miss = PrefixMiss(prefix, input, halved, output, settings);
                            if (miss == null)
                            {
                                return new Admission(halved, null, "");
                            }
                        }
                        return new Admission(-1, miss, "An earlier input no longer fits beside this one at any batch size", PrefixDrop: true);
                    }
                    return new Admission(-1, CompactionReason.ManagedBound, "No batch size fits both caps");
                }
            }
            catch (OverflowException e)
            {
                return new Admission(-1, CompactionReason.BoundOverflow, e.Message);
            }
        }

        // Null when all fit; a native miss is the writer's growth
        private static CompactionReason? PrefixMiss(IReadOnlyList<InputSummary> prefix, InputSummary input, int candidate, OutputShape output, AdmissionSettings settings)
        {
            var shared = Shared(prefix, input, input.OutBatch[candidate], output, settings);
            foreach (var summary in prefix.Append(input))
            {
                var peak = Peaks(summary, summary == input ? candidate : summary.Candidate, shared, prefix, input, output, settings);
                if (peak.Native > settings.NativeCap)
                {
                    return CompactionReason.WriterBound;
                }
                if (peak.Managed > settings.ManagedCap)
                {
                    return CompactionReason.ManagedBound;
                }
            }
            return null;
        }

        private static (long Native, long Managed, long Cap, long Built, long Writer) Check(IReadOnlyList<InputSummary> prefix, InputSummary input, int candidate, OutputShape output, AdmissionSettings settings)
        {
            var shared = Shared(prefix, input, input.OutBatch[candidate], output, settings);
            var peak = Peaks(input, candidate, shared, prefix, input, output, settings);
            return (peak.Native, peak.Managed, shared.Cap, shared.Built, shared.Writer);
        }

        // Cap, Built and W_group over the prefix with this input's output batch
        private static SharedTerms Shared(IReadOnlyList<InputSummary> prefix, InputSummary input, long inputOut, OutputShape output, AdmissionSettings settings)
        {
            checked
            {
                var outMax = inputOut;
                foreach (var earlier in prefix)
                {
                    outMax = Math.Max(outMax, earlier.OutBatch[earlier.Candidate]);
                }
                var cap = 2 * (settings.FlushBytes + outMax) + 64 * output.Buffers;
                var built = cap + 64 * output.Buffers;
                var caps = LevelCaps(prefix.Append(input), output, settings);
                // f_ℓ is Built times the largest share any input gives the leaf
                var loads = new LeafLoad[output.Leaves.Count];
                var length = settings.FlushBytes + outMax;
                var rows = (8 * length + output.RowBits - 1) / output.RowBits;
                for (int l = 0; l < loads.Length; l++)
                {
                    double share = 0, ratio = 0;
                    long value = 0, rowLevels = 1;
                    foreach (var summary in prefix.Append(input))
                    {
                        share = Math.Max(share, summary.LeafShare[l]);
                        ratio = Math.Max(ratio, summary.LeafLevelRatio[l]);
                        value = Math.Max(value, summary.LeafMeanValue[l]);
                        rowLevels = Math.Max(rowLevels, summary.LeafRowLevels[l]);
                    }
                    var leaf = output.Leaves[l];
                    // A byte more than the share, against rounding
                    var bytes = checked((long)Math.Ceiling(built * share) + 1);
                    if (leaf.Repeated && leaf.Shape != LeafShape.Bytes)
                    {
                        // Values and validity for Λ slots, in builders that at most double
                        var values = leaf.Shape == LeafShape.Bits ? (caps[l] + 7) / 8 : checked(leaf.Width * caps[l]);
                        bytes = Math.Min(bytes, Rounded(2 * values) + Rounded(2 * ((caps[l] + 7) / 8)));
                    }
                    var levels = leaf.Repeated ? Math.Min(checked((long)Math.Ceiling(length * ratio) + 1), caps[l]) : rows;
                    var valueWidth = leaf.Shape == LeafShape.Fixed ? leaf.Width : leaf.Shape == LeafShape.Bits ? 1 : value;
                    loads[l] = new LeafLoad(bytes, levels, leaf.Repeated, leaf.Repeated ? Math.Min(levels, rowLevels) : 1, valueWidth, leaf.Shape, leaf.Encoded, leaf.Decimal);
                }
                return new SharedTerms(cap, built, WriterBytes(settings.Compression, loads, rows));
            }
        }

        // A buffer's bytes after 64 B rounding and padding
        private static long Rounded(long bytes) => checked((bytes + 63) / 64 * 64 + 64);

        // Λ per list leaf: the copier closes a group before passing it
        public static long[] LevelCaps(IEnumerable<InputSummary> all, OutputShape output, AdmissionSettings settings)
        {
            var caps = new long[output.Leaves.Count];
            foreach (var summary in all)
            {
                for (int l = 0; l < caps.Length; l++)
                {
                    // At least an input group, a quarter of F's levels and 2^20
                    var quarter = checked((long)Math.Ceiling(settings.FlushBytes * summary.LeafLevelRatio[l] / 4));
                    caps[l] = output.Leaves[l].Repeated ? Math.Max(caps[l], Math.Max(Math.Max(summary.LeafGroupLevels[l], 1L << 20), quarter)) : 0;
                }
            }
            return caps;
        }

        // NativePeak_i(B) and ManagedPeak_i(B) under the shared writer terms
        private static (long Native, long Managed) Peaks(InputSummary summary, int candidate, SharedTerms shared, IReadOnlyList<InputSummary> prefix, InputSummary input, OutputShape output, AdmissionSettings settings)
        {
            var terms = Terms(summary, candidate, shared, prefix.Append(input).ToList(), output, settings);
            return (Math.Max(terms.NativeCopy, terms.NativePrecision), terms.Managed);
        }

        // The writer terms of a frozen prefix, every input at its batch
        public static SharedTerms Freeze(IReadOnlyList<InputSummary> frozen, OutputShape output, AdmissionSettings settings)
        {
            var last = frozen[^1];
            return Shared(frozen.Take(frozen.Count - 1).ToList(), last, last.OutBatch[last.Candidate], output, settings);
        }

        // One input's terms beside the others; summaries counts every one of them
        public static InputTerms Terms(InputSummary summary, int candidate, SharedTerms shared, IReadOnlyList<InputSummary> all, OutputShape output, AdmissionSettings settings)
        {
            checked
            {
                var groups = 64 + (summary.Out + settings.FlushBytes - 1) / settings.FlushBytes + 1;
                var persistent = NativeConstants.WriterFixed + NativeConstants.WriterCodec + NativeConstants.WriterChunkMetadata * output.Leaves.Count * groups;
                var fixedTerms = summary.Metadata + NativeConstants.ReaderFixed + persistent;
                var copy = fixedTerms + Math.Max(summary.ReadPeak[candidate], summary.ReaderOnly[candidate] + shared.Built + shared.Writer);
                var precision = summary.PrecisionDecode > 0 ? fixedTerms + summary.PrecisionDecode : 0;
                long statistics = 0, summaries = 0;
                for (int l = 0; l < output.Leaves.Count; l++)
                {
                    var largest = all.Max(x => x.LeafMaxValue[l]);
                    statistics += StatisticsPerLeaf + (output.Leaves[l].Shape == LeafShape.Bytes ? 2 * largest : 0);
                }
                foreach (var each in all)
                {
                    summaries += each.Bytes;
                }
                var dv = 64 + 8_256 * ((summary.Rows + 65_535) / 65_536);
                var transient = Math.Max(InputSummary.BatchRows(candidate) + summary.SynthStage[candidate], summary.PrecisionManaged);
                var managed = shared.Cap + shared.Cap / 2 + statistics + summary.Model + summaries + transient + settings.TransportChunk + dv;
                return new InputTerms(copy, precision, groups, statistics, transient, dv, managed);
            }
        }

        // The floor's largest native part names the decline
        private static (CompactionReason, string) Dominant(IReadOnlyList<InputSummary> prefix, InputSummary input, (long Native, long Managed, long Cap, long Built, long Writer) floor, OutputShape output, AdmissionSettings settings)
        {
            var groups = 64 + (input.Out + settings.FlushBytes - 1) / settings.FlushBytes + 1;
            var without = Shared(prefix, input, 0, output, settings);
            var growth = Math.Max(0, floor.Built + floor.Writer - without.Built - without.Writer);
            var outBatch = Math.Max(1, input.OutBatch[InputSummary.Candidates - 1]);
            var synthesized = (long)((double)growth * input.Floor.SynthOut / outBatch);
            var repeated = (long)((double)growth * input.Floor.RepeatedOut / outBatch);
            // Flat values come from the pages or dictionary pages
            var flat = growth - synthesized - repeated + input.Floor.ReadFlat;
            var pages = input.Floor.Decode - input.Floor.Dictionary;
            var dictionaryFlat = input.Floor.Dictionary > pages ? flat : 0;
            var parts = new (CompactionReason Reason, long Bytes)[]
            {
                // The builders and writer at F, before this input's batch
                (CompactionReason.WriterBound, without.Built + without.Writer),
                (CompactionReason.MetadataBound, input.Metadata + NativeConstants.WriterChunkMetadata * output.Leaves.Count * groups),
                (CompactionReason.DictionaryBound, input.Floor.Dictionary + dictionaryFlat),
                (CompactionReason.RowGroupCompressed, pages + flat - dictionaryFlat),
                (CompactionReason.RepeatedChunk, input.Floor.ReadRepeated + repeated),
                (CompactionReason.SynthesisBound, input.Floor.Synth + synthesized)
            };
            return (parts.MaxBy(x => x.Bytes).Reason, ": " + string.Join(", ", parts.Select(x => $"{CompactionReasons.Name(x.Reason)} {x.Bytes}")));
        }

        // Σ Dict_L, plus Ω_g when a dictionary is the largest buffer
        private static long Dictionary(DataFileScan scan, int group)
        {
            long total = 0, largest = 0, overlap = 0;
            foreach (var leaf in scan.Groups[group])
            {
                total += leaf.Dictionary;
                largest = Math.Max(largest, leaf.Dictionary);
                overlap = Math.Max(overlap, Math.Max(leaf.Stream, leaf.Buffer));
            }
            return largest > overlap ? total + largest : total;
        }

        // W for one group, the largest leaf's terms with its dictionary encoder
        // Outputs write one column chunk at a time
        public static long WriterBytes(Compression compression, IReadOnlyList<LeafLoad> leaves, long rows)
        {
            checked
            {
                long largest = 0;
                foreach (var leaf in leaves)
                {
                    // A repeated leaf's write batch runs on to the next row
                    var batch = DeltaParquetCompression.OutputWriteBatch + (leaf.Repeated ? leaf.RowLevels : 0);
                    var batchMax = Math.Min(leaf.Bytes, batch * Math.Max(1, leaf.Value));
                    var terms = WriterTerms.Leaf(compression, DeltaParquetCompression.OutputPageSize, leaf.Bytes, leaf.Levels, batchMax, leaf.RowLevels, leaf.Repeated ? 2 : 1, rows);
                    // Booleans are never dictionary encoded
                    var held = terms.Held + (leaf.Shape == LeafShape.Bits ? 0 : Dictionary(compression, leaf, batch, batchMax));
                    largest = Math.Max(largest, WriterTerms.Group(held, terms.Sink, terms.Overlap, 8 * leaf.Levels + rows));
                }
                return largest;
            }
        }

        // W_actual at flush, from the builders' levels, longest rows and write-batch windows
        public static Func<BuilderSizes, long, long> FlushWriter(OutputShape output, Compression compression)
        {
            var loads = new LeafLoad[output.Leaves.Count];
            return (sizes, rows) =>
            {
                Debug.Assert(sizes.Leaves.Count == loads.Length, "The writers measure the table's leaves in order");
                for (int l = 0; l < loads.Length; l++)
                {
                    var leaf = output.Leaves[l];
                    var size = sizes.Leaves[l];
                    var rowLevels = leaf.Repeated ? size.RowLevels : 1;
                    var batch = DeltaParquetCompression.OutputWriteBatch + (leaf.Repeated ? rowLevels : 0);
                    var value = leaf.Shape switch { LeafShape.Fixed => leaf.Width, LeafShape.Bits => 1, _ => Math.Max(1, (size.Window + batch - 1) / batch) };
                    loads[l] = new LeafLoad(size.Built, size.Levels, leaf.Repeated, rowLevels, value, leaf.Shape, leaf.Encoded, leaf.Decimal);
                }
                return WriterBytes(compression, loads, rows);
            };
        }

        // The dictionary encoder: memo, page and buffered indices
        internal static long Dictionary(Compression compression, LeafLoad leaf, long batch, long batchMax)
        {
            checked
            {
                const long limit = DeltaParquetCompression.OutputDictionaryPageSize;
                // The limit is checked after each batch, so a batch can pass it
                var entries = Math.Min(leaf.Levels, (limit - 1) / Math.Max(1, leaf.Encoded) + batch);
                // The hash quadruples past half full, the old table live meanwhile
                long slots = 1024;
                while (slots <= 2 * entries)
                {
                    slots *= 4;
                }
                var binary = leaf.Decimal || leaf.Shape == LeafShape.Bytes;
                var hash = (binary || leaf.Encoded != 8 ? 16 : 24) * (slots > 1024 ? slots + slots / 4 : slots);
                long memo = 0, page = entries * leaf.Encoded;
                if (binary)
                {
                    // A binary memo keeps its values, offsets and validity in doubling builders
                    var values = leaf.Decimal ? page : Math.Min(leaf.Bytes, limit + batchMax);
                    memo = 12 * (Math.Max(entries, 1024) + 1) + 3 * Math.Max(values, 4096) + 3 * Math.Max(entries, 1024) / 8;
                    page = leaf.Decimal ? page : Math.Min(limit - 1 + 4 * batch + batchMax, 4 * entries + values);
                }
                var pages = page + (compression == Compression.Uncompressed ? 0 : NativeConstants.Compressed(compression, page));
                // Indices wait for their data page, which closes by 20,000 rows
                var indices = leaf.Repeated ? Math.Min(leaf.Levels, Math.Min(WriterTerms.RowsPerPage * Math.Max(1, leaf.RowLevels), 4 * DeltaParquetCompression.OutputPageSize) + batch) : Math.Min(leaf.Levels, WriterTerms.RowsPerPage);
                var width = Math.Max(1, (long)Math.Ceiling(Math.Log2(Math.Max(entries, 2))));
                var indexPage = 2 + (indices + 7) / 8 * (1 + width) + 64 * width;
                return hash + memo + pages + 12 * indices + indexPage;
            }
        }

        // Per output leaf, maxima over an input's groups
        private sealed class OutputTotals
        {
            // The group being added: unrounded bytes and level slots
            private readonly long[] _raw;
            private readonly long[] _slots;
            private long _total;

            public OutputTotals(int leaves)
            {
                Share = new double[leaves];
                LevelRatio = new double[leaves];
                MaxValue = new long[leaves];
                MeanValue = new long[leaves];
                RowLevels = new long[leaves];
                GroupLevels = new long[leaves];
                _raw = new long[leaves];
                _slots = new long[leaves];
            }

            public double[] Share { get; }

            public double[] LevelRatio { get; }

            public long[] MaxValue { get; }

            public long[] MeanValue { get; }

            public long[] RowLevels { get; }

            public long[] GroupLevels { get; }

            public void Add(int leaf, long raw, long slots)
            {
                _raw[leaf] += raw;
                _slots[leaf] += slots;
            }

            public void AddBytes(long raw) => _total = checked(_total + raw);

            // Maxima over groups, so a dense group isn't averaged away
            public void EndGroup()
            {
                for (int l = 0; l < _raw.Length && _total > 0; l++)
                {
                    Share[l] = Math.Max(Share[l], (double)_raw[l] / _total);
                    LevelRatio[l] = Math.Max(LevelRatio[l], (double)_slots[l] / _total);
                    GroupLevels[l] = Math.Max(GroupLevels[l], _slots[l]);
                }
                Array.Clear(_raw);
                Array.Clear(_slots);
                _total = 0;
            }
        }

        // One group's output batch at u rows
        private readonly struct OutputTerms
        {
            private readonly DataFileScan _scan;
            private readonly int _group;
            private readonly long _rows;

            public OutputTerms(DataFileScan scan, int group, long rows)
            {
                _scan = scan;
                _group = group;
                _rows = rows;
            }

            // Out_g, adding per-leaf totals and parts when asked
            public long Out(BindingPlan plan, OutputTotals? totals, long[]? parts = null)
            {
                checked
                {
                    long total = 0;
                    var leaf = 0;
                    foreach (var column in plan.Columns)
                    {
                        total += OutNode(column, Length(column, _rows), totals, parts, ref leaf, 0, false);
                    }
                    return total;
                }
            }

            // Synth_g: every buffer FileBinding.NullData makes
            public long Synth(BindingPlan plan, out long largest)
            {
                checked
                {
                    long total = 0;
                    largest = 0;
                    foreach (var column in plan.Columns)
                    {
                        total += SynthNode(column, Length(column, _rows), ref largest);
                    }
                    return total;
                }
            }

            // u_k: the file's length, else the parent's, 0 under synthesized lists
            private long Length(PlanNode node, long parent)
            {
                if (node.Source == null)
                {
                    return parent;
                }
                return node.Source.Repeated ? _scan.LargestValues(_group, node.Source) : _rows;
            }

            // above is the slots of lists and maps over the node
            private long OutNode(PlanNode node, long u, OutputTotals? totals, long[]? parts, ref int leaf, long above, bool repeated)
            {
                checked
                {
                    var raw = (u + 7) / 8 + (node.HasOffsets ? 4 * (u + 1) : 0);
                    var bytes = Allocated((u + 7) / 8) + (node.HasOffsets ? Allocated(4 * (u + 1)) : 0);
                    var part = node.Source == null ? 0 : node.Source.Repeated ? 1 : 2;
                    if (node.IsLeaf)
                    {
                        long values = 0, max = 0, mean = 0;
                        if (node.Shape == LeafShape.Fixed)
                        {
                            values = node.Width * u;
                        }
                        else if (node.Shape == LeafShape.Bits)
                        {
                            values = (u + 7) / 8;
                        }
                        else if (node.Source != null)
                        {
                            var terms = _scan.Groups[_group][_scan.Position(node.Source.Leaf)];
                            values = Math.Min(DataFileScan.Bytes(terms), u * terms.MaxValue);
                            max = terms.MaxValue;
                            mean = (values + Math.Max(1, u) - 1) / Math.Max(1, u);
                        }
                        bytes += Allocated(values);
                        if (parts != null)
                        {
                            parts[part] += bytes;
                        }
                        if (totals != null)
                        {
                            totals.Add(leaf, raw + values, u + above);
                            totals.AddBytes(raw + values);
                            totals.MaxValue[leaf] = Math.Max(totals.MaxValue[leaf], max);
                            totals.MeanValue[leaf] = Math.Max(totals.MeanValue[leaf], mean);
                            totals.RowLevels[leaf] = Math.Max(totals.RowLevels[leaf], repeated ? Math.Max(1, (u + _rows - 1) / Math.Max(1, _rows)) : 1);
                        }
                        leaf++;
                        return bytes;
                    }
                    if (parts != null)
                    {
                        parts[part] += bytes;
                    }
                    totals?.AddBytes(raw);
                    var collection = node.Type.Type is SchemaType.Array or SchemaType.Map;
                    foreach (var child in node.Children)
                    {
                        var childLength = child.Source != null ? Length(child, u) : node.Type.Type == SchemaType.Struct ? u : 0;
                        bytes += OutNode(child, childLength, totals, parts, ref leaf, collection ? above + u : above, repeated || collection);
                    }
                    return bytes;
                }
            }

            private long SynthNode(PlanNode node, long length, ref long largest)
            {
                checked
                {
                    if (node.Source != null)
                    {
                        long total = 0;
                        foreach (var child in node.Children)
                        {
                            total += SynthNode(child, child.Source != null ? Length(child, length) : length, ref largest);
                        }
                        return total;
                    }
                    var validity = (length + 7) / 8;
                    var offsets = node.HasOffsets ? 4 * (length + 1) : 0;
                    var values = node.IsLeaf ? node.Shape switch { LeafShape.Fixed => node.Width * length, LeafShape.Bits => (length + 7) / 8, _ => 0 } : 0;
                    largest = Math.Max(largest, Math.Max(validity, Math.Max(offsets, values)));
                    var bytes = Allocated(validity) + Allocated(offsets) + Allocated(values);
                    foreach (var child in node.Children)
                    {
                        bytes += SynthNode(child, node.Type.Type == SchemaType.Struct ? length : 0, ref largest);
                    }
                    return bytes;
                }
            }

            // a(x): rounded to 64 B plus the allocator's 64 B
            private static long Allocated(long bytes) => bytes == 0 ? 0 : checked((bytes + 63) / 64 * 64 + 64);
        }
    }
}
