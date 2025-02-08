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

using Apache.Arrow.Compression;
using Apache.Arrow.Ipc;
using BenchmarkDotNet.Attributes;
using FASTER.core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZstdSharp;

namespace FlowtideDotNet.Benchmarks
{
    public class ArrowSerializeBenchmark
    {
        EventBatchData? _eventBatchData;
        EventBatchSerializer eventBatchSerializer = new EventBatchSerializer();
        Apache.Arrow.RecordBatch? _recordBatch;
        MemoryStream _memoryStream = new MemoryStream();
        private ArrayBufferWriter<byte> _bufferWriter = new ArrayBufferWriter<byte>();
        private byte[]? _toDeserialize;
        private BatchCompressor _batchCompressor = new BatchCompressor();
        private CompressionCodecFactory _compressionCodecFactory = new CompressionCodecFactory();
        private byte[]? _toDeserializeCompressed;
        private BatchDecompressor _batchDecompressor = new BatchDecompressor();
        private byte[] _diskDestinationBuffer = new byte[16 * 1024 * 1024];
        private BatchDictionaryCompressor? _batchDictionaryCompressor;
        private Compressor _compressor = new Compressor(CompressionLevel);
        private byte[]? _compressedByteArray;
        private Decompressor _decompressor = new Decompressor();
        private byte[]? _dictionaryCompressedByteArray;
        private BatchDictionaryDecompressor? _batchDictionaryDecompressor;
        private byte[]? _version12CompressedBytes;
        private byte[]? _version12Bytes;

        private const int CompressionLevel = 3;

        [GlobalSetup]
        public void GlobalSetup()
        {
            Random r = new Random(123);
            Column[] columns = new Column[3];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }

            for (int i = 0; i < 8000; i++)
            {
                columns[0].Add(new Int64Value(r.Next(0, 100_000)));
                columns[1].Add(new Int64Value(r.Next(0, 100_000)));

                // Add a string from random ascii
                byte[] bytes = new byte[10];
                for (int j = 0; j < bytes.Length; j++)
                    bytes[j] = (byte)r.Next(32, 127);
                columns[2].Add(new StringValue(Encoding.UTF8.GetString(bytes)));
            }

            _eventBatchData = new EventBatchData(columns);
            _recordBatch = EventArrowSerializer.BatchToArrow(_eventBatchData, _eventBatchData.Count);

            _bufferWriter.ResetWrittenCount();
            eventBatchSerializer.SerializeEventBatch(_bufferWriter, _eventBatchData, _eventBatchData.Count);
            _toDeserialize = _bufferWriter.WrittenSpan.ToArray();
            _bufferWriter.ResetWrittenCount();
            _batchCompressor = new BatchCompressor();

            eventBatchSerializer.SerializeEventBatch(_bufferWriter, _eventBatchData, _eventBatchData.Count, _batchCompressor);
            _toDeserializeCompressed = _bufferWriter.WrittenSpan.ToArray();
            _bufferWriter.ResetWrittenCount();

            var dictionaries = GenerateDictionaries();
            _batchDictionaryCompressor = new BatchDictionaryCompressor(dictionaries);
            _batchDictionaryDecompressor = new BatchDictionaryDecompressor(dictionaries);
            _bufferWriter.ResetWrittenCount();

            // Create an array where the entire record batch is compressed with zstd
            var serializationEstimate = eventBatchSerializer.GetSerializationEstimation(_eventBatchData);
            var written = eventBatchSerializer.SerializeEventBatch(_diskDestinationBuffer.AsSpan(), serializationEstimate, _eventBatchData, _eventBatchData.Count);
            var bound = Compressor.GetCompressBound(written);
            var destSpan = _bufferWriter.GetSpan(bound);
            var compressedSize = _compressor.Wrap(_diskDestinationBuffer.AsSpan(0, written), destSpan);
            _bufferWriter.Advance(compressedSize);
            _compressedByteArray = _bufferWriter.WrittenSpan.ToArray();
            _bufferWriter.ResetWrittenCount();

            eventBatchSerializer.SerializeEventBatch(_bufferWriter, _eventBatchData, _eventBatchData.Count, _batchDictionaryCompressor);
            _dictionaryCompressedByteArray = _bufferWriter.WrittenSpan.ToArray();
            _bufferWriter.ResetWrittenCount();

            // Create the result from version 12 serialization to use for deserialize benchmark
            using var version12compressedStream = new MemoryStream();
            // Compression added to the stream
            var zlibStream = new ZLibStream(version12compressedStream, CompressionMode.Compress);
            var recordBatch = EventArrowSerializer.BatchToArrow(_eventBatchData, _eventBatchData.Count);
            var batchWriter = new ArrowStreamWriter(zlibStream, recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(recordBatch);
            batchWriter.Dispose();
            zlibStream.Dispose();

            _version12CompressedBytes = version12compressedStream.ToArray();

            using var version12Stream = new MemoryStream();
            // Compression added to the stream
            recordBatch = EventArrowSerializer.BatchToArrow(_eventBatchData, _eventBatchData.Count);
            batchWriter = new ArrowStreamWriter(version12Stream, recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(recordBatch);
            batchWriter.Dispose();

            _version12Bytes = version12Stream.ToArray();

        }

        private byte[][] GenerateDictionaries()
        {
            var training = new BatchTrainingCompressor(3);
            Random r = new Random(124);
            for (int b = 0; b < 100; b++)
            {
                Column[] columns = new Column[3];
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i] = Column.Create(GlobalMemoryManager.Instance);
                }

                for (int i = 0; i < 8000; i++)
                {
                    columns[0].Add(new Int64Value(r.Next(0, 100_000)));
                    columns[1].Add(new Int64Value(r.Next(0, 100_000)));

                    // Add a string from random ascii
                    byte[] bytes = new byte[10];
                    for (int j = 0; j < bytes.Length; j++)
                        bytes[j] = (byte)r.Next(32, 127);
                    columns[2].Add(new StringValue(Encoding.UTF8.GetString(bytes)));
                }
                var batch = new EventBatchData(columns);
                eventBatchSerializer.SerializeEventBatch(_bufferWriter, batch, batch.Count, training);
                _bufferWriter.ResetWrittenCount();
            }
            return training.GetDictionaries();
        }

        [Benchmark(Description = "Flowtide Serialize (Ratio = 1.0)")]
        public void FlowtideSerializer()
        {
            Debug.Assert(_eventBatchData != null);
            _bufferWriter.ResetWrittenCount();
            eventBatchSerializer.SerializeEventBatch(_bufferWriter, _eventBatchData, _eventBatchData.Count);
        }

        [Benchmark(Description = "Flowtide Deserialize")]
        public void FlowtideDeserialize()
        {
            Debug.Assert(_toDeserialize != null);
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(_toDeserialize));
            var deserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance);
            var batch = deserializer.DeserializeBatch(ref reader).EventBatch;
            batch.Dispose();
        }

        [Benchmark(Description = "Arrow Serialize (Ratio = 1.0)")]
        public void ArrowSerializer()
        {
            Debug.Assert(_recordBatch != null);
            _memoryStream.SetLength(0);
            var batchWriter = new ArrowStreamWriter(_memoryStream, _recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(_recordBatch);
        }

        [Benchmark(Description = "Arrow Deserialize")]
        public void ArrowDeserialize()
        {
            Debug.Assert(_toDeserialize != null);
            var stream = new MemoryStream(_toDeserialize);
            var reader = new ArrowStreamReader(stream);
            var batch = reader.ReadNextRecordBatch();
            batch.Dispose();
        }

        [Benchmark(Description = "Arrow Deserialize Convert To Flowtide")]
        public void ArrowDeserializeConvertToFlowtide()
        {
            Debug.Assert(_toDeserialize != null);
            var stream = new MemoryStream(_toDeserialize);
            var reader = new ArrowStreamReader(stream);
            var batch = reader.ReadNextRecordBatch();
            EventArrowSerializer.ArrowToBatch(batch, GlobalMemoryManager.Instance);
            batch.Dispose();
        }

        [Benchmark(Description = "Convert Flowtide To Arrow Serialize")]
        public void ConvertFlowtideToArrowSerialize()
        {
            Debug.Assert(_eventBatchData != null);
            _memoryStream.SetLength(0);
            var recordBatch = EventArrowSerializer.BatchToArrow(_eventBatchData, _eventBatchData.Count);
            var batchWriter = new ArrowStreamWriter(_memoryStream, recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(recordBatch);
        }

        // Compression benchmarks

        private class BatchDictionaryCompressor : IBatchCompressor
        {
            private Compressor[] compressors;
            private int currentColumn;

            public BatchDictionaryCompressor(byte[][] dictionaries)
            {
                compressors = new Compressor[dictionaries.Length];
                for (int i = 0; i < dictionaries.Length; i++)
                {
                    compressors[i] = new Compressor(CompressionLevel);
                    compressors[i].LoadDictionary(dictionaries[i]);
                }
            }

            public void ColumnChange(int columnIndex)
            {
                currentColumn = columnIndex;
            }

            public int Wrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return compressors[currentColumn].Wrap(input, output);
            }
        }

        private class BatchCompressor : IBatchCompressor
        {
            private Compressor compressor;

            public BatchCompressor()
            {
                compressor = new Compressor(CompressionLevel);
            }

            public void ColumnChange(int columnIndex)
            {
            }

            public int Wrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return compressor.Wrap(input, output);
            }
        }

        private class BatchTrainingCompressor : IBatchCompressor
        {
            List<byte[]>[] samples;
            int currentColumn;
            public BatchTrainingCompressor(int columnCount)
            {
                samples = new List<byte[]>[columnCount];
                for (int i = 0; i < columnCount; i++)
                {
                    samples[i] = new List<byte[]>();
                }
            }

            public void ColumnChange(int columnIndex)
            {
                currentColumn = columnIndex;
            }

            public int Wrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                samples[currentColumn].Add(input.ToArray());
                return 0;
            }

            public byte[][] GetDictionaries()
            {
                return samples.Select(x => DictBuilder.TrainFromBufferFastCover(x, CompressionLevel).ToArray()).ToArray();
            }
        }

        private class BatchDecompressor : IBatchDecompressor
        {
            private Decompressor decompressor;

            public BatchDecompressor()
            {
                decompressor = new Decompressor();
            }

            public void ColumnChange(int columnIndex)
            {
            }

            public int Unwrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return decompressor.Unwrap(input, output);
            }
        }

        private class BatchDictionaryDecompressor : IBatchDecompressor
        {
            private Decompressor[] decompressor;
            private int currentColumn;

            public BatchDictionaryDecompressor(byte[][] dictionaries)
            {
                decompressor = new Decompressor[dictionaries.Length];
                for (int i = 0; i < dictionaries.Length; i++)
                {
                    decompressor[i] = new Decompressor();
                    decompressor[i].LoadDictionary(dictionaries[i]);
                }
            }

            public void ColumnChange(int columnIndex)
            {
                currentColumn = columnIndex;
            }

            public int Unwrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return decompressor[currentColumn].Unwrap(input, output);
            }
        }

        [Benchmark(Description = "Flowtide Serialize Compressed Buffers (Ratio = 1.82)")]
        public void FlowtideSerializeCompressed()
        {
            Debug.Assert(_eventBatchData != null);
            _bufferWriter.ResetWrittenCount();
            eventBatchSerializer.SerializeEventBatch(_bufferWriter, _eventBatchData, _eventBatchData.Count, _batchCompressor);
        }

        [Benchmark(Description = "Flowtide Serialize Compressed Buffers With Dictionary (Ratio 2.14)")]
        public void FlowtideSerializeCompressedWithDictionary()
        {
            Debug.Assert(_eventBatchData != null);
            _bufferWriter.ResetWrittenCount();
            eventBatchSerializer.SerializeEventBatch(_bufferWriter, _eventBatchData, _eventBatchData.Count, _batchDictionaryCompressor);
        }

        [Benchmark(Description = "Arrow Serialize Compressed Buffers (Ratio = 1.82)")]
        public void ArrowSerializeCompressed()
        {
            Debug.Assert(_recordBatch != null);
            _memoryStream.SetLength(0);
            var batchWriter = new ArrowStreamWriter(_memoryStream, _recordBatch.Schema, true, new IpcOptions()
            {
                CompressionCodec = CompressionCodecType.Zstd,
                CompressionLevel = CompressionLevel,
                CompressionCodecFactory = _compressionCodecFactory
            });
            batchWriter.WriteRecordBatch(_recordBatch);
        }

        [Benchmark(Description = "Flowtide Deserialize Compressed Buffers")]
        public void FlowtideDeserializeCompressed()
        {
            Debug.Assert(_toDeserializeCompressed != null);
            var deserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance, _batchDecompressor);
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(_toDeserializeCompressed));
            var batch = deserializer.DeserializeBatch(ref reader).EventBatch;
            batch.Dispose();
        }

        [Benchmark(Description = "Flowtide Deserialize Compressed Buffers With Dictionary")]
        [BenchmarkCategory("Deserialize"), BenchmarkCategory("Compressed")]
        public void FlowtideDeserializeCompressedWithDictionary()
        {
            Debug.Assert(_toDeserializeCompressed != null);
            var deserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance, _batchDictionaryDecompressor);
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(_toDeserializeCompressed));
            var batch = deserializer.DeserializeBatch(ref reader).EventBatch;
            batch.Dispose();
        }

        [Benchmark(Description = "Arrow Deserialize Compressed Buffers")]
        public void ArrowDeserializeCompressed()
        {
            Debug.Assert(_toDeserializeCompressed != null);
            var stream = new MemoryStream(_toDeserializeCompressed);
            var reader = new ArrowStreamReader(stream, _compressionCodecFactory);
            var batch = reader.ReadNextRecordBatch();
            batch.Dispose();
        }

        /// <summary>
        /// Benchmark that simulates the actions done in version 12
        /// </summary>
        [Benchmark(Description = "Serialize in version 12 Compressed (Ratio = 2.26)")]
        public void SerializeInVersion12Compressed()
        {
            Debug.Assert(_eventBatchData != null);
            // A new memory stream was created for each batch
            using var stream = new MemoryStream();
            // Compression added to the stream
            using var zlibStream = new ZLibStream(stream, CompressionMode.Compress);
            var recordBatch = EventArrowSerializer.BatchToArrow(_eventBatchData, _eventBatchData.Count);
            var batchWriter = new ArrowStreamWriter(zlibStream, recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(recordBatch);

            // Bytes from the memory stream to return from the serialize method
            var bytes = stream.ToArray();
            // Copy the bytes into a buffer that was then written to disk
            bytes.AsSpan().CopyTo(_diskDestinationBuffer.AsSpan());
        }

        [Benchmark(Description = "Serialize in version 12 (Ratio = 1.0)")]
        public void SerializeInVersion12()
        {
            Debug.Assert(_eventBatchData != null);
            // A new memory stream was created for each batch
            using var stream = new MemoryStream();
            var recordBatch = EventArrowSerializer.BatchToArrow(_eventBatchData, _eventBatchData.Count);
            var batchWriter = new ArrowStreamWriter(stream, recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(recordBatch);

            // Bytes from the memory stream to return from the serialize method
            var bytes = stream.ToArray();
            // Copy the bytes into a buffer that was then written to disk
            bytes.AsSpan().CopyTo(_diskDestinationBuffer.AsSpan());
        }

        [Benchmark(Description = "Deserialize in version 12 compressed")]
        public void DeserializeInVersion12Compressed()
        {
            Debug.Assert(_version12CompressedBytes != null);
            // The incoming data is first copied into its own byte array
            var arr = _version12CompressedBytes.ToArray();
            using var memoryStream = new MemoryStream(arr);

            // The stream is then passed to the decompression function
            var stream = new ZLibStream(memoryStream, CompressionMode.Decompress);
            // The stream is then passed to the binary reader
            var reader = new BinaryReader(stream);

            var arrowReader = new ArrowStreamReader(reader.BaseStream);

            var nextBatch = arrowReader.ReadNextRecordBatch();
            var flowtideBatch = EventArrowSerializer.ArrowToBatch(nextBatch, GlobalMemoryManager.Instance);
            nextBatch.Dispose();
            flowtideBatch.Dispose();
        }

        [Benchmark(Description = "Deserialize in version 12")]
        public void DeserializeInVersion12()
        {
            Debug.Assert(_version12Bytes != null);
            // The incoming data is first copied into its own byte array
            var arr = _version12Bytes.ToArray();
            using var memoryStream = new MemoryStream(arr);

            // The stream is then passed to the binary reader
            using var reader = new BinaryReader(memoryStream);

            var arrowReader = new ArrowStreamReader(reader.BaseStream);

            var nextBatch = arrowReader.ReadNextRecordBatch();
            var flowtideBatch = EventArrowSerializer.ArrowToBatch(nextBatch, GlobalMemoryManager.Instance);
            nextBatch.Dispose();
            flowtideBatch.Dispose();
        }

        [Benchmark(Description = "Serialize then compress entire body (Ratio = 1.72)")]
        public void SerializeThenCompress()
        {
            Debug.Assert(_eventBatchData != null);

            _bufferWriter.ResetWrittenCount();
            var serializationEstimate = eventBatchSerializer.GetSerializationEstimation(_eventBatchData);
            var written = eventBatchSerializer.SerializeEventBatch(_diskDestinationBuffer.AsSpan(), serializationEstimate, _eventBatchData, _eventBatchData.Count);
            var bound = Compressor.GetCompressBound(written);

            var destSpan = _bufferWriter.GetSpan(bound);
            var compressedSize = _compressor.Wrap(_diskDestinationBuffer.AsSpan(0, written), destSpan);
            _bufferWriter.Advance(compressedSize);
        }

        [Benchmark(Description = "Deserialize compressed entire body")]
        public void DeserializeFullCompressedBatch()
        {
            _decompressor.Unwrap(_compressedByteArray.AsSpan(), _diskDestinationBuffer.AsSpan());
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(_diskDestinationBuffer));
            var deserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance);
            var batch = deserializer.DeserializeBatch(ref reader).EventBatch;
            batch.Dispose();
        }
    }
}
