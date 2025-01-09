using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.SqlServer.Data
{
    public class StreamPageDataReader : DbDataReader
    {
        private StreamPage[]? _source;
        private readonly string[] _memberNames;

        public static StreamPageDataReader Create(StreamPage[] source)
        {
            return new StreamPageDataReader(source);
        }

        public StreamPageDataReader(StreamPage[] source)
        {
            _source = source ?? throw new ArgumentOutOfRangeException(nameof(source));
            _memberNames = ["PageKey", "StreamKey", "PageId", "Payload", "Version"];
        }

        StreamPage? _current;


        public override int Depth => 0;

        public override DataTable GetSchemaTable()
        {
            return new DataTable()
            {
                Columns =
                {
                    { "PageKey", typeof(Guid) },
                    { "StreamKey", typeof(int) },
                    { "PageId", typeof(long) },
                    { "Payload", typeof(byte[]) },
                    { "Version", typeof(int) },
                }
            };
        }

        public override void Close()
        {
            Shutdown();
        }

        public override bool HasRows => _active;

        private bool _active = true;
        public override bool NextResult()
        {
            _active = false;
            return false;
        }

        private int _index;
        public override bool Read()
        {
            if (_active)
            {
                if (_source?.Length > _index)
                {
                    _current = _source[_index];
                    _index++;
                    return true;
                }
                else
                {
                    _active = false;
                }
            }

            _current = null;
            return false;
        }

        public override int RecordsAffected
        {
            get { return 0; }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                Shutdown();
            }
        }

        private void Shutdown()
        {
            _active = false;
            _current = null;
            _source = null;
        }

        public override int FieldCount
        {
            get { return _memberNames.Length; }
        }
        public override bool IsClosed
        {
            get
            {
                return _source == null;
            }
        }

        public override bool GetBoolean(int ordinal)
        {
            return (bool)this[ordinal];
        }

        public override byte GetByte(int ordinal)
        {
            return (byte)this[ordinal];
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            Debug.Assert(buffer != null);
            byte[] s = (byte[])this[ordinal];
            int available = s.Length - (int)dataOffset;
            if (available <= 0) return 0;

            int count = Math.Min(length, available);
            Buffer.BlockCopy(s, (int)dataOffset, buffer, bufferOffset, count);
            return count;
        }

        public override char GetChar(int ordinal)
        {
            return (char)this[ordinal];
        }

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            Debug.Assert(buffer != null);
            string s = (string)this[ordinal];
            int available = s.Length - (int)dataOffset;
            if (available <= 0) return 0;

            int count = Math.Min(length, available);
            s.CopyTo((int)dataOffset, buffer, bufferOffset, count);
            return count;
        }

        protected override DbDataReader GetDbDataReader(int ordinal)
        {
            throw new NotSupportedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            return GetFieldType(ordinal).Name;
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)this[ordinal];
        }

        public override decimal GetDecimal(int ordinal)
        {
            return (decimal)this[ordinal];
        }

        public override double GetDouble(int ordinal)
        {
            return (double)this[ordinal];
        }

        public override Type GetFieldType(int ordinal)
        {
            return ordinal switch
            {
                0 => typeof(Guid),
                1 => typeof(int),
                2 => typeof(long),
                3 => typeof(byte[]),
                4 => typeof(int),
                _ => typeof(DBNull),
            };
        }

        public override float GetFloat(int ordinal)
        {
            return (float)this[ordinal];
        }

        public override Guid GetGuid(int ordinal)
        {
            return (Guid)this[ordinal];
        }

        public override short GetInt16(int ordinal)
        {
            return (short)this[ordinal];
        }

        public override int GetInt32(int ordinal)
        {
            return (int)this[ordinal];
        }

        public override long GetInt64(int ordinal)
        {
            return (long)this[ordinal];
        }

        public override string GetName(int ordinal)
        {
            return _memberNames[ordinal];
        }

        public override int GetOrdinal(string name)
        {
            return Array.IndexOf(_memberNames, name);
        }

        public override string GetString(int ordinal)
        {
            return (string)this[ordinal];
        }

        public override object GetValue(int ordinal)
        {
            return this[ordinal];
        }

        public override IEnumerator GetEnumerator() => new DbEnumerator(this);

        public override int GetValues(object[] values)
        {
            Debug.Assert(_current != null);
            values[0] = _current.PageKey;
            values[1] = _current.StreamKey;
            values[2] = _current.PageId;
            values[3] = _current.Payload;
            values[4] = _current.Version;

            return 5;
        }

        public override bool IsDBNull(int ordinal)
        {
            return this[ordinal] is DBNull;
        }

        public override object this[string name]
        {
            get
            {
                return this[GetOrdinal(name)];
            }

        }

        public override object this[int ordinal]
        {
            get
            {
                Debug.Assert(_current != null);
                return ordinal switch
                {
                    0 => _current.PageKey,
                    1 => _current.StreamKey,
                    2 => _current.PageId,
                    3 => _current.Payload,
                    4 => _current.Version,
                    _ => DBNull.Value,
                };
            }
        }
    }
}