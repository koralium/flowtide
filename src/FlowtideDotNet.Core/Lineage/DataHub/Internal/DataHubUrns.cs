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

namespace FlowtideDotNet.Core.Lineage.DataHub.Internal
{
    internal static class DataHubUrns
    {
        public const string Orchestrator = "flowtide";

        // GMS rejects longer urns, measured URL encoded.
        public const int MaxUrnLength = 512;

        public static string Platform(string platform)
        {
            return "urn:li:dataPlatform:" + Encode(platform);
        }

        public static string PlatformInstance(string platform, string instance)
        {
            return $"urn:li:dataPlatformInstance:({Platform(platform)},{Encode(instance)})";
        }

        public static string Dataset(string platform, string qualifiedName, string env)
        {
            return $"urn:li:dataset:({Platform(platform)},{Encode(qualifiedName)},{env})";
        }

        public static string DataFlow(string flowId, string cluster)
        {
            return $"urn:li:dataFlow:({Orchestrator},{Encode(flowId)},{Encode(cluster)})";
        }

        public static string DataJob(string flowUrn, string jobId)
        {
            return $"urn:li:dataJob:({flowUrn},{Encode(jobId)})";
        }

        public static string SchemaField(string datasetUrn, string fieldPath)
        {
            return $"urn:li:schemaField:({datasetUrn},{Encode(fieldPath)})";
        }

        // Length of the Java URLEncoder form GMS validates.
        public static int UrlEncodedLength(string value)
        {
            var length = 0;
            foreach (var b in Encoding.UTF8.GetBytes(value))
            {
                var unreserved = (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') ||
                    b == '.' || b == '-' || b == '*' || b == '_' || b == ' ';
                length += unreserved ? 1 : 3;
            }
            return length;
        }

        // Same reserved characters as the DataHub UrnEncoder.
        public static string Encode(string value)
        {
            if (value.AsSpan().IndexOfAny(",()␟") < 0)
            {
                return value;
            }
            var builder = new StringBuilder(value.Length + 8);
            foreach (var c in value)
            {
                switch (c)
                {
                    case ',':
                        builder.Append("%2C");
                        break;
                    case '(':
                        builder.Append("%28");
                        break;
                    case ')':
                        builder.Append("%29");
                        break;
                    case '␟':
                        builder.Append("%E2%90%9F");
                        break;
                    default:
                        builder.Append(c);
                        break;
                }
            }
            return builder.ToString();
        }
    }
}
