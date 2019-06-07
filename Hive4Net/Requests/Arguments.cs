using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Requests
{
    public class Arguments<T> where T : TBase, new()
    {
        private readonly Dictionary<Type, string> _nameList = new Dictionary<Type, string>()
        {
            {typeof(CloseOperationReq), "CloseOperation_args" },
            {typeof(FetchResultsReq), "FetchResults_args" },
            {typeof(OpenSessionRequest), "OpenSession_args" },
            {typeof(GetResultSetMetadataReq), "GetResultSetMetadata_args" },
            {typeof(CloseSessionReq), "CloseSession_args" },
            {typeof(ExecuteStatementReq), "ExecuteStatementReq_args" }
        };
        private T _req;

        public T Req
        {
            get
            {
                return _req;
            }
            set
            {
                _isSet.req = true;
                this._req = value;
            }
        }

        public Isset _isSet;

        public struct Isset
        {
            public bool req;
        }

        public Arguments()
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
                    case 1:
                        if (field.Type == TType.Struct)
                        {
                            Req = new T();
                            await Req.ReadAsync(protocol, cancellationToken);
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
            if (Req != null && _isSet.req)
            {
                field.Name = "req";
                field.Type = TType.Struct;
                field.ID = 1;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await Req.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            string name = _nameList[typeof(T)];
            StringBuilder sb = new StringBuilder($"{name}(");
            sb.Append("Req: ");
            sb.Append(Req == null ? "<null>" : Req.ToString());
            sb.Append(")");
            return sb.ToString();
        }
    }
}
