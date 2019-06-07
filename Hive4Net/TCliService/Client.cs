using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.Requests;
using Hive4Net.Responses;
using Thrift;
using Thrift.Protocols;
using Thrift.Protocols.Entities;

namespace Hive4Net.TCliService
{
    public class Client : IDisposable
    {
        private readonly Dictionary<Type, string> _messageList = new Dictionary<Type, string>()
        {
            {typeof(CloseOperationReq), "CloseOperation" },
            {typeof(GetResultSetMetadataReq), "GetResultSetMetadata" },
            {typeof(FetchResultsReq), "FetchResults" },
            {typeof(OpenSessionRequest), "OpenSession" },
            {typeof(CloseSessionReq), "CloseSession" },
            {typeof(ExecuteStatementReq), "ExecuteStatement" }
        };

        private readonly Thrift.Protocols.TProtocol _outProtocol;
        private readonly Thrift.Protocols.TProtocol _inProtocol;
        private readonly int _sequenceId;

        public Client(TProtocol protocol) : this(protocol, protocol)
        {
        }

        public Client(TProtocol inProtocol, TProtocol outProtocol)
        {
            _inProtocol = inProtocol;
            _outProtocol = outProtocol;
            _sequenceId = 0;
        }

        #region " IDisposable Support "

        private bool _IsDisposed;

        // IDisposable
        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_IsDisposed)
            {
                if (disposing)
                {
                    ((IDisposable) _inProtocol)?.Dispose();

                    ((IDisposable) _outProtocol)?.Dispose();
                }
            }

            _IsDisposed = true;
        }

        #endregion " IDisposable Support "

        public async Task<TResp> GenericRequestAsync<TResp, TReq>(TReq request)
            where TResp : TBase, new()
            where TReq : TBase, new()
        {
            await SendAsync<TReq>(request);
            return await ReceiveAsync<TResp>();
        }

        private async Task SendAsync<T>(T request) where T : TBase, new()
        {
            string message = _messageList[typeof(T)];
            await _outProtocol.WriteMessageBeginAsync(new TMessage(message, TMessageType.Call,
                _sequenceId));
            Arguments<T> args = new Arguments<T> {Req = request};
            await args.WriteAsync(_outProtocol, CancellationToken.None);
            await _outProtocol.WriteMessageEndAsync();
            await _outProtocol.Transport.FlushAsync();
        }

        private async Task<T> ReceiveAsync<T>() where T : TBase, new()
        {
            TMessage msg = await _inProtocol.ReadMessageBeginAsync();
            if (msg.Type == TMessageType.Exception)
            {
                TApplicationException x = await TApplicationException.ReadAsync(_inProtocol, CancellationToken.None);
                await _inProtocol.ReadMessageEndAsync();
                throw x;
            }

            Result<T> result = new Result<T>();
            await result.ReadAsync(_inProtocol, CancellationToken.None);
            await _inProtocol.ReadMessageEndAsync();
            if (result._isSet.success)
            {
                return result.Success;
            }

            throw new TApplicationException(TApplicationException.ExceptionType.MissingResult,
                "GetResultSetMetadata failed: unknown result");
        }

        public async Task<OpenSessionResp> OpenSessionAsync(OpenSessionRequest request)
        {
            return await GenericRequestAsync<OpenSessionResp, OpenSessionRequest>(request);
        }

        public async Task<FetchResultsResp> FetchResultsAsync(FetchResultsReq request)
        {
            return await GenericRequestAsync<FetchResultsResp, FetchResultsReq>(request);
        }

        public async Task<GetResultSetMetadataResp> GetResultSetMetadataAsync(GetResultSetMetadataReq request)
        {
            return await GenericRequestAsync<GetResultSetMetadataResp, GetResultSetMetadataReq>(request);
        }

        public async Task<CloseOperationResp> CloseOperationAsync(CloseOperationReq request)
        {
            return await GenericRequestAsync<CloseOperationResp, CloseOperationReq>(request);
        }


        public async Task<CloseSessionResp> CloseSessionAsync(CloseSessionReq request)
        {
            return await GenericRequestAsync<CloseSessionResp, CloseSessionReq>(request);
        }

        public async Task<ExecuteStatementResp> ExecuteStatementAsync(ExecuteStatementReq request)
        {
            return await GenericRequestAsync<ExecuteStatementResp, ExecuteStatementReq>(request);
        }

    }
}
