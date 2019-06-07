using System;
using System.Net;
using System.Threading.Tasks;
using Hive4Net.Datasets;
using Hive4Net.enums;
using Hive4Net.Requests;
using Hive4Net.Responses;
using Hive4Net.TCliService;
using Hive4Net.ThriftSasl;
using Thrift.Protocols;
using Thrift.Transports;
using Thrift.Transports.Client;

namespace Hive4Net
{
    public class Hive : IDisposable
    {
        private TClientTransport _transport;
        private ProtocolVersion _version;
        private Client _client;
        private SessionHandle _session;
        private string _userName;

        public Hive(string host, int port, string userName = "None", string password = "None",
            ProtocolVersion version = ProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7)
        {
            var socket = new TSocketClientTransport(IPAddress.Parse(host), port);
            _transport = new TSaslClientTransport(socket, userName, password);
            var protocol = new TBinaryProtocol(_transport);
            _client = new Client(protocol);
            _version = version;
            _userName = userName;
        }

        public Hive(TClientTransport transport, string userName = null, ProtocolVersion version = ProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7)
        {
            _transport = transport;
            var protocol = new TBinaryProtocol(_transport);
            _client = new Client(protocol);
            _version = version;
            _userName = userName;
        }

        public async Task OpenAsync()
        {
            if (_transport != null && !_transport.IsOpen)
            {
                await _transport.OpenAsync();
            }

            if (_session == null)
            {
                _session = await GetSessionAsync();
            }
        }

        public async Task<Cursor> GetCursorAsync()
        {
            await OpenAsync();
            return new Cursor(_session, _client, _version);
        }

        private async Task<SessionHandle> GetSessionAsync()
        {
            var openSessionRequest = new OpenSessionRequest(_version);
            var response = await _client.OpenSessionAsync(openSessionRequest);
            response.Status.CheckStatus();
            return response.SessionHandle;
        }

        public async Task CloseAsync()
        {
            if (_session != null)
                await CloseSessionAsync();
            if (_transport != null && _transport.IsOpen)
                _transport.Close();
        }

        private async Task CloseSessionAsync()
        {
            CloseSessionReq closeSessionReq = new CloseSessionReq();
            closeSessionReq.SessionHandle = _session;
            CloseSessionResp closeSessionResp = await _client.CloseSessionAsync(closeSessionReq);
            closeSessionResp.Status.CheckStatus();
        }

        protected virtual async Task Dispose(bool disposing)
        {
            if (disposing)
            {
                await CloseAsync();
                _client = null;
                _transport = null;
                _session = null;
            }
        }

        public void Dispose()
        {
            Dispose(true).Wait();
            GC.SuppressFinalize(this);
        }

    }
}
