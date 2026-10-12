// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using BenchmarkDotNet.Attributes;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Benchmarks;

/// <summary>
/// Real Reservoir commits, including pages retained by an operator and fetched before the walk.
/// RunIteration also exposes separate caller, pending-fetch and durable-checkpoint latencies.
/// Allocation comparisons must count all threads: the worker allocates outside the caller.
/// </summary>
[MemoryDiagnoser]
public class BackgroundCommitBenchmark
{
    [Params(256, 4096)]
    public int PageCount { get; set; }

    [Params(0, 10)]
    public int HeldPercent { get; set; }

    [Params(false, true)]
    public bool FetchPendingPage { get; set; }

    private ReservoirPersistentStorage _storage = null!;
    private StateManagerSync<StateManagerMetadata> _manager = null!;
    private IStateClient<Page, Metadata> _client = null!;
    private Meter _meter = null!;
    private long[] _keys = null!;
    private Page[] _held = null!;
    private TaskCompletionSource? _walkRelease;
    private string _directory = null!;

    public readonly record struct Timings(double CommitMicroseconds, double FetchMicroseconds, double CheckpointMicroseconds);

    [GlobalSetup]
    public async Task Setup()
    {
        _directory = Path.Combine(Path.GetTempPath(), "flowtide-background-benchmark", Guid.NewGuid().ToString("N"));
        _storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        _meter = new Meter("background-commit-benchmark");
        _manager = new StateManagerSync<StateManagerMetadata>(new StateManagerOptions
        {
            PersistentStorage = _storage,
            BackgroundCommit = true,
            CachePageCount = PageCount + 100,
            MinCachePageCount = PageCount + 100,
            TemporaryStorageOptions = new FileCacheOptions { DirectoryPath = _directory }
        }, NullLoggerFactory.Instance, _meter, "benchmark", GlobalMemoryManager.Instance);
        await _manager.InitializeAsync();
        await _manager.CacheTable.StopCleanupTask();
        _client = await _manager.CreateClientAsync<Page, Metadata>("pages",
            new StateClientOptions<Page> { ValueSerializer = new PageSerializer() }, GlobalMemoryManager.Instance);
        _keys = new long[PageCount];
        _held = new Page[PageCount * HeldPercent / 100];
        for (int i = 0; i < _keys.Length; i++)
        {
            _keys[i] = _client.GetNewPageId();
            _client.AddOrUpdate(_keys[i], new Page());
        }
        await _client.Commit();
        await _manager.CheckpointAsync();
        if (FetchPendingPage)
        {
            // A hook before claiming a page ensures fetch timing always covers an owed write.
            _manager.PageWriteHookForTests = (_, _) => _walkRelease?.Task ?? Task.CompletedTask;
        }
    }

    [Benchmark]
    public async Task<Timings> RunIteration()
    {
        for (int i = 0; i < _keys.Length; i++)
        {
            var page = (await _client.GetValue(_keys[i]))!;
            page.Value++;
            _client.AddOrUpdate(_keys[i], page);
            if (i < _held.Length)
            {
                _held[i] = page;
            }
            else
            {
                page.Return();
            }
        }
        _walkRelease = FetchPendingPage ? new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) : null;
        var start = Stopwatch.GetTimestamp();
        await _client.Commit();
        var committed = Stopwatch.GetTimestamp();
        double fetchUs = 0;
        try
        {
            if (FetchPendingPage)
            {
                var fetchStart = Stopwatch.GetTimestamp();
                var page = (await _client.GetValue(_keys[^1]))!;
                fetchUs = Microseconds(Stopwatch.GetTimestamp() - fetchStart);
                // Editing after the fetch must belong to the following generation.
                page.Value++;
                _client.AddOrUpdate(_keys[^1], page);
                page.Return();
            }
        }
        finally
        {
            _walkRelease?.TrySetResult();
            foreach (var page in _held)
            {
                page.Return();
            }
        }
        await _manager.CheckpointAsync();
        return new Timings(Microseconds(committed - start), fetchUs, Microseconds(Stopwatch.GetTimestamp() - start));
    }

    private static double Microseconds(long ticks) => ticks * (1_000_000.0 / Stopwatch.Frequency);

    [GlobalCleanup]
    public void Cleanup()
    {
        _walkRelease?.TrySetResult();
        _manager.Dispose();
        _storage.Dispose();
        _meter.Dispose();
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    private sealed class Metadata : IStorageMetadata
    {
        public bool Updated { get; set; }
    }

    private sealed class Page : ICacheObject
    {
        private int _rents = 1;
        public int Value;
        public bool RemovedFromCache { get; set; }
        public int RentCount => Volatile.Read(ref _rents);
        public bool TryRent()
        {
            var rents = Volatile.Read(ref _rents);
            while (rents != 0)
            {
                var observed = Interlocked.CompareExchange(ref _rents, rents + 1, rents);
                if (observed == rents) return true;
                rents = observed;
            }
            return false;
        }
        public void Return() => Interlocked.Decrement(ref _rents);
        public bool TryReclaimForEviction() => Interlocked.CompareExchange(ref _rents, 0, 1) == 1;
        public void EnterWriteLock() => Monitor.Enter(this);
        public void ExitWriteLock() => Monitor.Exit(this);
    }

    private sealed class PageSerializer : IStateSerializer<Page>
    {
        public void Serialize(in IBufferWriter<byte> writer, in Page page)
        {
            BinaryPrimitives.WriteInt32LittleEndian(writer.GetSpan(4), page.Value);
            writer.Advance(4);
        }
        public Page Deserialize(ReadOnlySequence<byte> bytes, int length)
        {
            var reader = new SequenceReader<byte>(bytes);
            if (!reader.TryReadLittleEndian(out int value)) throw new InvalidOperationException("Invalid page");
            return new Page { Value = value };
        }
        public void Serialize(in IBufferWriter<byte> writer, in ICacheObject page) => Serialize(writer, (Page)page);
        public ICacheObject DeserializeCacheObject(ReadOnlySequence<byte> bytes, int length) => Deserialize(bytes, length);
        public Task CheckpointAsync<T>(IStateSerializerCheckpointWriter writer, StateClientMetadata<T> metadata) where T : IStorageMetadata => Task.CompletedTask;
        public Task InitializeAsync<T>(IStateSerializerInitializeReader reader, StateClientMetadata<T> metadata) where T : IStorageMetadata => Task.CompletedTask;
        public void ClearTemporaryAllocations() { }
        public void Dispose() { }
    }
}
