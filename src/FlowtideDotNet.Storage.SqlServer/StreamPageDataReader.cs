using System.Collections;
using System.Data;
using System.Data.Common;

namespace FlowtideDotNet.Storage.SqlServer
{
    public class StreamPageDataReader : DbDataReader
    {
        private StreamPage[] _source;
        private readonly string[] _memberNames;

        public static StreamPageDataReader Create(StreamPage[] source)
        {
            return new StreamPageDataReader(source);
        }

        public StreamPageDataReader(StreamPage[] source)
        {
            _source = source ?? throw new ArgumentOutOfRangeException("source");
            _memberNames = ["PageKey", "StreamKey", "PageId", "Payload", "Version"];
        }

        StreamPage _current;


        public override int Depth
        {
            get { return 0; }
        }

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

        public override bool HasRows
        {
            get
            {
                return active;
            }
        }
        private bool active = true;
        public override bool NextResult()
        {
            active = false;
            return false;
        }

        private int _index;
        public override bool Read()
        {
            if (active)
            {
                if (_source.Length > _index)
                {
                    _current = _source[_index];
                    _index++;
                    return true;
                }
                else
                {
                    active = false;
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
            if (disposing) Shutdown();
        }
        private void Shutdown()
        {
            active = false;
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

        public override bool GetBoolean(int i)
        {
            return (bool)this[i];
        }

        public override byte GetByte(int i)
        {
            return (byte)this[i];
        }

        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            byte[] s = (byte[])this[i];
            int available = s.Length - (int)fieldOffset;
            if (available <= 0) return 0;

            int count = Math.Min(length, available);
            Buffer.BlockCopy(s, (int)fieldOffset, buffer, bufferoffset, count);
            return count;
        }

        public override char GetChar(int i)
        {
            return (char)this[i];
        }

        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            string s = (string)this[i];
            int available = s.Length - (int)fieldoffset;
            if (available <= 0) return 0;

            int count = Math.Min(length, available);
            s.CopyTo((int)fieldoffset, buffer, bufferoffset, count);
            return count;
        }

        protected override DbDataReader GetDbDataReader(int i)
        {
            throw new NotSupportedException();
        }

        public override string GetDataTypeName(int i)
        {
            return GetFieldType(i).Name;
        }

        public override DateTime GetDateTime(int i)
        {
            return (DateTime)this[i];
        }

        public override decimal GetDecimal(int i)
        {
            return (decimal)this[i];
        }

        public override double GetDouble(int i)
        {
            return (double)this[i];
        }

        public override Type GetFieldType(int i)
        {
            return i switch
            {
                0 => typeof(Guid),
                1 => typeof(int),
                2 => typeof(long),
                3 => typeof(byte[]),
                4 => typeof(int),
                _ => typeof(DBNull),
            };
        }

        public override float GetFloat(int i)
        {
            return (float)this[i];
        }

        public override Guid GetGuid(int i)
        {
            return (Guid)this[i];
        }

        public override short GetInt16(int i)
        {
            return (short)this[i];
        }

        public override int GetInt32(int i)
        {
            return (int)this[i];
        }

        public override long GetInt64(int i)
        {
            return (long)this[i];
        }

        public override string GetName(int i)
        {
            return _memberNames[i];
        }

        public override int GetOrdinal(string name)
        {
            return Array.IndexOf(_memberNames, name);
        }

        public override string GetString(int i)
        {
            return (string)this[i];
        }

        public override object GetValue(int i)
        {
            return this[i];
        }

        public override IEnumerator GetEnumerator() => new DbEnumerator(this);

        public override int GetValues(object[] values)
        {
            values[0] = _current.PageKey;
            values[1] = _current.StreamKey;
            values[2] = _current.PageId;
            values[3] = _current.Payload;
            values[4] = _current.Version;

            return 5;
        }

        public override bool IsDBNull(int i)
        {
            return this[i] is DBNull;
        }

        public override object this[string name]
        {
            get
            {
                return this[GetOrdinal(name)];
            }

        }

        public override object this[int i]
        {
            get
            {
                return i switch
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