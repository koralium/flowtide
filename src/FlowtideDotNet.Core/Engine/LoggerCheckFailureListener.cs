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

using FlowtideDotNet.Base.Engine;
using Microsoft.Extensions.Logging;
using SqlParser;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Resources;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Engine
{
    internal class LoggerCheckFailureListener : ICheckFailureListener
    {
        private readonly ILogger _logger;
        private readonly LogLevel _logLevel;

        public LoggerCheckFailureListener(ILogger logger, LogLevel logLevel)
        {
            _logger = logger;
            _logLevel = logLevel;
        }

        public void OnCheckFailure(ref readonly CheckFailureNotification notification)
        {
            _logger.Log(_logLevel, 0, new CheckFailureLog(notification.Message, notification.Tags.ToArray()), default, (state, exception) =>
            {
                return $"Check failed: " + state.ToString();
            });
        }

        private struct CheckFailureLog : IReadOnlyList<KeyValuePair<string, object?>>
        {
            private readonly string _originalMessage;
            private readonly KeyValuePair<string, object?>[] _tags;

            public CheckFailureLog(string originalMessage, KeyValuePair<string, object?>[] tags)
            {
                _originalMessage = originalMessage;
                _tags = tags;
            }

            public KeyValuePair<string, object?> this[int index]
            {
                get
                {
                    if (index == Count - 1)
                    {
                        return new KeyValuePair<string, object?>("{OriginalFormat}", _originalMessage);
                    }
                    return _tags[index];
                }
            }

            public int Count => _tags.Length + 1;

            public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
            {
                for (int i = 0; i < Count; ++i)
                {
                    yield return this[i];
                }
            }

            public override string ToString()
            {
                var msg = _originalMessage;
                for (int i = 0; i < _tags.Length; i++)
                {
                    msg = msg.Replace($"{{{_tags[i].Key}}}", _tags[i].Value?.ToString() ?? "null", StringComparison.OrdinalIgnoreCase);
                }
                return msg;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
