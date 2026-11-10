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

using System.Text;
using System.Text.Json.Serialization;

namespace FlowtideDotNet.Connector.StarRocks.Internal.HttpApi
{
    /// <summary>
    /// Fields shared by stream load and transaction responses.
    /// </summary>
    internal abstract class LoadResponse
    {
        [JsonPropertyName("Status")]
        [JsonConverter(typeof(StarRocksStatusConverter))]
        public StarRocksStatus Status { get; set; }

        [JsonPropertyName("Message")]
        public string? Message { get; set; }

        [JsonPropertyName("NumberTotalRows")]
        public long? NumberTotalRows { get; set; }

        [JsonPropertyName("NumberLoadedRows")]
        public long? NumberLoadedRows { get; set; }

        [JsonPropertyName("NumberFilteredRows")]
        public long? NumberFilteredRows { get; set; }

        [JsonPropertyName("NumberUnselectedRows")]
        public long? NumberUnselectedRows { get; set; }

        [JsonPropertyName("ErrorURL")]
        public string? ErrorUrl { get; set; }

        /// <summary>
        /// Rejected rows fetched from <see cref="ErrorUrl"/>, null if unavailable.
        /// </summary>
        [JsonIgnore]
        public string? ErrorLog { get; set; }

        // StarRocks only links an error log for rejected rows
        [JsonIgnore]
        public bool HasRejectedRows => ErrorUrl != null;

        /// <summary>
        /// Message plus row counts and rejected rows, for the user.
        /// </summary>
        public string GetErrorMessage()
        {
            var builder = new StringBuilder();
            builder.Append(string.IsNullOrEmpty(Message) ? $"Load failed with status '{Status}'" : Message);

            if (NumberFilteredRows > 0)
            {
                builder.Append($" (total rows: {NumberTotalRows}, loaded: {NumberLoadedRows}, filtered: {NumberFilteredRows}, unselected: {NumberUnselectedRows})");
            }

            if (ErrorLog != null)
            {
                builder.Append(". Rejected rows:");
                builder.AppendLine();
                builder.Append(ErrorLog);
            }
            else if (ErrorUrl != null)
            {
                builder.Append($". Error log: {ErrorUrl}");
            }
            return builder.ToString();
        }
    }
}
