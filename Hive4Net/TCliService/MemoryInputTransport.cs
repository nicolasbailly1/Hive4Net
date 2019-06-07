using System;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Transports;

namespace Hive4Net.TCliService
{
    public class MemoryInputTransport : TClientTransport
    {
        private byte[] buf_;
        private int pos_;
        private int endPos_;

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
            buf_ = buf;
            pos_ = offset;
            endPos_ = offset + length;
        }

        public void Clear()
        {
            buf_ = null;
        }

        public override void Close()
        {
        }

        public override bool IsOpen
        {
            get { return true; }
        }

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
                Array.Copy(buf_, pos_, buf, off, amtToRead);
                ConsumeBuffer(amtToRead);
            }
            return amtToRead;
        }

        public override async Task WriteAsync(byte[] buf, int off, int len, CancellationToken cancellationToken)
        {
        }

        public byte[] GetBuffer()
        {
            return buf_;
        }

        public int GetBufferPosition
        {
            get { return pos_; }
        }

        public int GetBytesRemainingInBuffer
        {
            get { return endPos_ - pos_; }
        }

        public void ConsumeBuffer(int len)
        {
            pos_ += len;
        }

        protected override void Dispose(bool disposing)
        {
            Dispose();
        }
    }
}
