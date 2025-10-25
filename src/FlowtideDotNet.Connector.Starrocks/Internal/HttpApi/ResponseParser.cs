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

using System.IO.Pipelines;
using System.Text.Json;

namespace FlowtideDotNet.Connector.StarRocks.Internal.HttpApi
{
    internal class ResponseParser
    {
        public ResponseParser()
        {
            
        }

        public static async Task<QueryResult> ParseQuery(Stream stream)
        {
            var enumerable = Parse(stream);

            var enumerator = enumerable.GetAsyncEnumerator();

            if (!await enumerator.MoveNextAsync())
            {
                throw new InvalidOperationException("No data");
            }

            if (!enumerator.Current.IsConnectionId)
            {
                throw new InvalidOperationException("Expected connectionid");
            }

            if (!await enumerator.MoveNextAsync())
            {
                throw new InvalidOperationException("No data");
            }

            if (!enumerator.Current.IsMetadata)
            {
                throw new InvalidOperationException("Expected metadata");
            }

            var metaList = enumerator.Current.Meta;
            List<SchemaField> fields = new List<SchemaField>();
            foreach(var field in metaList)
            {
                if (field.Name == null)
                {
                    throw new InvalidOperationException("Metadata field missing name");
                }
                if (field.Type == null)
                {
                    throw new InvalidOperationException("Metadata field missing type");
                }
                fields.Add(new SchemaField(field.Name, field.Type));
            }

            return new QueryResult(fields, ReadDataRows(enumerator));
        }

        private static async IAsyncEnumerable<IReadOnlyList<object?>> ReadDataRows(IAsyncEnumerator<HttpResultRow> enumerator)
        {
            while(await enumerator.MoveNextAsync())
            {
                var current = enumerator.Current;

                if (current.IsData)
                {
                    yield return current.Data;
                }
            }
        }

        public static async IAsyncEnumerable<HttpResultRow> Parse(Stream stream)
        {
            var reader = PipeReader.Create(stream);

            while (true)
            {
                ReadResult result = await reader.ReadAsync().ConfigureAwait(false);

                if (result.IsCompleted)
                {
                    break;
                }

                if (result.Buffer.Length == 1 && result.Buffer.FirstSpan[0] == '\n')
                {
                    reader.AdvanceTo(result.Buffer.End);
                    continue;
                }

                if (TryReadJson(in result, out var pos))
                {
                    // Found an entire JSON object parse it
                    var row = ParseHttpResult(result, pos);
                    if (row == null)
                    {
                        throw new InvalidOperationException("Failed to parse HTTP result row");
                    }
                    yield return row;
                    reader.AdvanceTo(pos);
                }
                else
                {
                    reader.AdvanceTo(result.Buffer.Start, pos);
                }
            }
        }

        private static HttpResultRow? ParseHttpResult(ReadResult readResult, SequencePosition end)
        {
            var slice = readResult.Buffer.Slice(0, end);
            var reader = new Utf8JsonReader(slice);

            return JsonSerializer.Deserialize<HttpResultRow>(ref reader);
        }

        private static bool TryReadJson(in ReadResult readResult, out SequencePosition pos)
        {
            var reader = new Utf8JsonReader(readResult.Buffer);
            
            if (!reader.Read())
            {
                pos = readResult.Buffer.End;
                return false;
            }

            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new InvalidOperationException("Expected start of object");
            }

            //Skip the object
            var skipResult = reader.TrySkip();

            if (!skipResult)
            {
                pos = readResult.Buffer.End;
            }
            else
            {
                pos = reader.Position;
            }
            return skipResult;
        }
    }
}
