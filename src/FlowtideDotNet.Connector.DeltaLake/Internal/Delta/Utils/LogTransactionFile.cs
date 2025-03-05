using Stowage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils
{
    internal class LogTransactionFile
    {
        public string FileName { get; }

        public bool IsCheckpoint { get; }

        public bool IsJson { get; }

        public long Version { get; }

        public IOEntry IOEntry { get; }

        public bool IsCompacted { get; }

        public LogTransactionFile(string fileName, bool isCheckpoint, bool isJson, long version, IOEntry ioEntry, bool isCompacted)
        {
            FileName = fileName;
            IsCheckpoint = isCheckpoint;
            IsJson = isJson;
            Version = version;
            IOEntry = ioEntry;
            IsCompacted = isCompacted;
        }

    }
}
