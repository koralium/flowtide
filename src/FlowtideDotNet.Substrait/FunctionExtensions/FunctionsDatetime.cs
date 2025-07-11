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

namespace FlowtideDotNet.Substrait.FunctionExtensions
{
    public static class FunctionsDatetime
    {
        public const string Uri = "/functions_datetime.yaml";
        public const string Strftime = "strftime";
        public const string GetTimestamp = "gettimestamp";
        public const string FloorTimestampDay = "floor_timestamp_day";
        public const string ParseTimestamp = "parse_timestamp";
        public const string Extract = "extract";
        public const string Format = "format";
        public const string TimestampAdd = "timestamp_add";
    }
}
