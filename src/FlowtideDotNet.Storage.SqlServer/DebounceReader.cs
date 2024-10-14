using Microsoft.Data.SqlClient;
using System.Collections.Concurrent;

namespace FlowtideDotNet.Storage.SqlServer
{
    public class DebounceQueue : IDisposable
    {
        private readonly ConcurrentQueue<Guid> _queue = new();
        private readonly ConcurrentDictionary<Guid, byte[]> _set = new();
        private readonly string _connectionString;
        private int? _streamKey;
        private bool disposedValue;
        private readonly CancellationTokenSource _cancellationSource;

        public DebounceQueue(string connectionString)
        {
            _connectionString = connectionString;
            _cancellationSource = new();
            Task.Run(Execute);
        }

        public void Enqueue(Guid pageKey)
        {
            _queue.Enqueue(pageKey);
        }

        public void Init(int streamKey)
        {
            _streamKey = streamKey;
        }

        public byte[] WaitForValue(Guid pageKey)
        {
            while (!_set.ContainsKey(pageKey))
            {
                Thread.Sleep(1);
            }

            return _set[pageKey];
        }

        private async Task Execute()
        {
            while (!_cancellationSource.IsCancellationRequested)
            {
                if (_queue.IsEmpty)
                {
                    Thread.Sleep(50);
                    continue;
                }

                using var connection = new SqlConnection(_connectionString);
                using var command = new SqlCommand();
                command.Parameters.AddWithValue("@StreamKey", _streamKey);
                command.Connection = connection;

                var i = 0;
                var parameters = new List<string>();
                while (_queue.TryDequeue(out Guid key))
                {
                    var k = $"@param{i}";
                    parameters.Add(k);
                    command.Parameters.AddWithValue(k, key);
                    i++;
                    if (i > 50)
                    {
                        break;
                    }
                }

                command.CommandText = $"SELECT PageKey, Payload FROM StreamPages WHERE StreamKey = @StreamKey AND PageKey IN({string.Join(", ", parameters)})";

                // Here we assume that we are reading a single value. Adjust based on your needs.
                try
                {

                    await connection.OpenAsync();
                    var reader = await command.ExecuteReaderAsync();

                    while (await reader.ReadAsync())
                    {
                        var key = await reader.GetFieldValueAsync<Guid>(0);
                        var payload = await reader.GetFieldValueAsync<byte[]>(1);
                        _set.AddOrUpdate(key, payload, (k, v) => payload);
                    }
                }
                catch (Exception ex)
                {

                    throw;
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                    _queue.Clear();
                    _set.Clear();
                    _cancellationSource.Cancel();
                    _cancellationSource.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~DebounceQueue()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}