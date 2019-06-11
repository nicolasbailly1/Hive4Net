using System;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Transports;

namespace Hive4Net.TCliService
{
    public class MemoryInputTransport : TClientTransport
    {
        private byte[] _buf;
        private int _pos;
        private int _endPos;

        public MemoryInputTransport()
        {
        }

        public MemoryInputTransport(byte[] buf)
        {
            Reset(buf);
        }

        public MemoryInputTransport(byte[] buf, int offset, int length)
        {
            Reset(buf, offset, length);
        }

        public void Reset(byte[] buf)
        {
            Reset(buf, 0, buf.Length);
        }

        public void Reset(byte[] buf, int offset, int length)
        {
            _buf = buf;
            _pos = offset;
            _endPos = offset + length;
        }

        public void Clear()
        {
            _buf = null;
        }

        public override void Close()
        {
        }

        public override bool IsOpen => true;

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
        }

        public override async Task<int> ReadAsync(byte[] buf, int off, int len, CancellationToken cancellationToken)
        {
            int bytesRemaining = GetBytesRemainingInBuffer;
            int amtToRead = (len > bytesRemaining ? bytesRemaining : len);
            if (amtToRead > 0)
            {
                Array.Copy(_buf, _pos, buf, off, amtToRead);
                ConsumeBuffer(amtToRead);
            }
            return amtToRead;
        }

        public override async Task WriteAsync(byte[] buf, int off, int len, CancellationToken cancellationToken)
        {
        }

        public byte[] GetBuffer()
        {
            return _buf;
        }

        public int GetBufferPosition => _pos;

        public int GetBytesRemainingInBuffer => _endPos - _pos;

        public void ConsumeBuffer(int len)
        {
            _pos += len;
        }

        protected override void Dispose(bool disposing)
        {
            Dispose();
        }
    }
}
