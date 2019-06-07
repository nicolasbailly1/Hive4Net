using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.enums;
using Hive4Net.TCliService;
using Thrift.Transports;
using Thrift.Transports.Client;

namespace Hive4Net.ThriftSasl
{
    public class TSaslClientTransport: TClientTransport, IDisposable
    {
        protected SaslClient _sasl;
        protected TSocketClientTransport _socket;
        protected static int STATUS_BYTES = 1;
        protected static int PAYLOAD_LENGTH_BYTES = 4;
        protected List<byte> statusBytes = new List<byte>() { ((byte)0x01), ((byte)0x02), ((byte)0x03), ((byte)0x04), ((byte)0x05) };
        private bool _IsOpen = false;
        private byte[] _MessageHeader = new byte[STATUS_BYTES + PAYLOAD_LENGTH_BYTES];
        private ByteArrayOutputStream _WriteBuffer = new ByteArrayOutputStream();

        public TSaslClientTransport(TSocketClientTransport socket, string userName, string password)
        {
            _sasl = new SaslClient(socket.Host.ToString(), new PlainMechanism(userName, password));
            _socket = socket;
        }

        public void Dispose()
        {
            _socket.Close();
            _socket = null;
            _sasl.Dispose();
        }

        public override void Close()
        {
            _socket.Close();
            _sasl.Dispose();
        }

        public override bool IsOpen
        {
            get { return _IsOpen; }
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            if (!IsOpen)
            {
                await this._socket.OpenAsync(cancellationToken);
                await SendSaslMsgAsync(SaslStatus.START, _sasl.Mechanism, cancellationToken);
                await SendSaslMsgAsync(SaslStatus.OK, _sasl.process(null), cancellationToken);

                while (true)
                {
                    var result = await ReceiveSaslMsgAsync(cancellationToken);
                    if (result.Status == SaslStatus.COMPLETE)
                    {
                        _IsOpen = true;
                        break;
                    }
                    else if (result.Status == SaslStatus.OK)
                    {
                        await SendSaslMsgAsync(SaslStatus.OK, _sasl.process(Encoding.UTF8.GetBytes(result.Body)), cancellationToken);
                    }
                    else
                    {
                        this._socket.Close();
                        throw new Exception($"Bad SASL negotiation status: {result.Status} ({result.Body})");
                    }
                }
            }
        }

        public async Task SendSaslMsgAsync(SaslStatus status, string body, CancellationToken cancellationToken)
        {
            await SendSaslMsgAsync(status, Encoding.UTF8.GetBytes(body), cancellationToken);
        }

        public async Task SendSaslMsgAsync(SaslStatus status, byte[] body, CancellationToken cancellationToken)
        {
            _MessageHeader[0] = statusBytes[(int)status - 1];
            Utils.EncodeBigEndian(body.Length, _MessageHeader, STATUS_BYTES);
            await _socket.WriteAsync(_MessageHeader, cancellationToken);
            await _socket.WriteAsync(body, cancellationToken);
            await _socket.FlushAsync(cancellationToken);
        }

        public class SaslMsg
        {
            public SaslStatus Status { get; set; }
            public string Body { get; set; }
            public SaslMsg(SaslStatus status, string body)
            {
                Status = status;
                Body = body;
            }

            public SaslMsg()
            {
            }
        }

        public async Task<SaslMsg> ReceiveSaslMsgAsync(CancellationToken cancellationToken)
        {
            SaslMsg result = new SaslMsg();
            await _socket.ReadAllAsync(_MessageHeader, 0, _MessageHeader.Length, cancellationToken);
            result.Status = (SaslStatus)(statusBytes.IndexOf(_MessageHeader[0]) + 1);
            byte[] body = new byte[Utils.DecodeBigEndian(_MessageHeader, STATUS_BYTES)];
            await _socket.ReadAllAsync(body, 0, body.Length, cancellationToken);

            result.Body = Encoding.UTF8.GetString(body);
            return result;
        }

        public async Task<int> ReadLengthAsync(CancellationToken cancellationToken)
        {
            byte[] lenBuf = new byte[4];
            await _socket.ReadAllAsync(lenBuf, 0, lenBuf.Length, cancellationToken);
            return Utils.DecodeBigEndian(lenBuf);
        }

        public async Task WriteLengthAsync(int length, CancellationToken cancellationToken)
        {
            byte[] lenBuf = new byte[4];
            Utils.EncodeFrameSize(length, lenBuf);
            await _socket.WriteAsync(lenBuf, cancellationToken);
        }

        MemoryInputTransport readBuffer = new MemoryInputTransport();
        public override async Task<int> ReadAsync(byte[] buf, int off, int len, CancellationToken cancellationToken)
        {
            int length = await readBuffer.ReadAsync(buf, off, len, cancellationToken);
            if (length > 0)
                return length;

            await ReadFrameAsync(cancellationToken);
            int i = await readBuffer.ReadAsync(buf, off, len, cancellationToken);
            return i;
        }

        private async Task ReadFrameAsync(CancellationToken cancellationToken)
        {
            int dataLength = await ReadLengthAsync(cancellationToken);
            if (dataLength < 0)
                throw new TTransportException("Read a negative frame size (" + dataLength + ")!");

            byte[] buff = new byte[dataLength];
            await _socket.ReadAllAsync(buff, 0, dataLength, cancellationToken);
            //string s = Encoding.UTF8.GetString(buff);
            readBuffer.Reset(buff);
        }


        public override async Task WriteAsync(byte[] buf, int off, int len, CancellationToken cancellationToken)
        {
            _WriteBuffer.Write(buf, off, len);
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            byte[] buff = _WriteBuffer.GetBytes();
            _WriteBuffer.Reset();
            await WriteLengthAsync(buff.Length, cancellationToken);
           await _socket.WriteAsync(buff, 0, buff.Length, cancellationToken);
           await _socket.FlushAsync(cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            Dispose();
        }
    }
}
