using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.Responses;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Requests
{
    public class Result<T> where T : TBase, new()
    {
        private readonly Dictionary<Type, string> _nameList = new Dictionary<Type, string>()
        {
            {typeof(CloseOperationResp), "CloseOperation_result" },
            {typeof(FetchResultsResp), "FetchResults_result" },
            {typeof(OpenSessionResp), "OpenSession_result" },
            {typeof(GetResultSetMetadataResp), "GetResultSetMetadata_result" },
            {typeof(CloseSessionResp), "CloseSessionResp_result" },
            {typeof(ExecuteStatementResp), "ExecuteStatementResp_result" }
        };

        private T _success;

        public T Success
        {
            get => _success;
            set
            {
                _isSet.success = true;
                this._success = value;
            }
        }

        public Isset _isSet;

        public struct Isset
        {
            public bool success;
        }

        public Result()
        {
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TField field;
            await protocol.ReadStructBeginAsync(cancellationToken);
            while (true)
            {
                field = await protocol.ReadFieldBeginAsync(cancellationToken);
                if (field.Type == TType.Stop)
                {
                    break;
                }
                switch (field.ID)
                {
                    case 0:
                        if (field.Type == TType.Struct)
                        {
                            Success = new T();
                            await Success.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    default:
                        await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        break;
                }
                await protocol.ReadFieldEndAsync(cancellationToken);
            }
            await protocol.ReadStructEndAsync(cancellationToken);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            string name = _nameList[typeof(T)];
            TStruct struc = new TStruct(name);
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();

            if (this._isSet.success)
            {
                if (Success != null)
                {
                    field.Name = "Success";
                    field.Type = TType.Struct;
                    field.ID = 0;
                    await protocol.WriteFieldBeginAsync(field, cancellationToken);
                    await Success.WriteAsync(protocol, cancellationToken);
                    await protocol.WriteFieldEndAsync(cancellationToken);
                }
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            string name = _nameList[typeof(T)];
            StringBuilder sb = new StringBuilder($"{name}(");
            sb.Append("Success: ");
            sb.Append(Success == null ? "<null>" : Success.ToString());
            sb.Append(")");
            return sb.ToString();
        }
    }
}
