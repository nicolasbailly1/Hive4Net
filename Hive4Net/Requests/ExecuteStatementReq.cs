using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.TCliService;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Requests
{
    public partial class ExecuteStatementReq : TBase
    {
        private Dictionary<string, string> _confOverlay;
        private bool _runAsync;

        public SessionHandle SessionHandle { get; set; }

        public string Statement { get; set; }

        public Dictionary<string, string> ConfOverlay
        {
            get => _confOverlay;
            set
            {
                _isSet.confOverlay = true;
                this._confOverlay = value;
            }
        }

        public bool RunAsync
        {
            get => _runAsync;
            set
            {
                _isSet.runAsync = true;
                this._runAsync = value;
            }
        }

        public Isset _isSet;

        public struct Isset
        {
            public bool confOverlay;
            public bool runAsync;
        }

        public ExecuteStatementReq()
        {
            this._runAsync = false;
            this._isSet.runAsync = true;
        }

        public ExecuteStatementReq(SessionHandle sessionHandle, string statement) : this()
        {
            this.SessionHandle = sessionHandle;
            this.Statement = statement;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_sessionHandle = false;
            bool isset_statement = false;
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
                            SessionHandle = new SessionHandle();
                            await SessionHandle.ReadAsync(protocol);
                            isset_sessionHandle = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 2:
                        if (field.Type == TType.String)
                        {
                            Statement = await protocol.ReadStringAsync(cancellationToken);
                            isset_statement = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 3:
                        if (field.Type == TType.Map)
                        {
                            {
                                ConfOverlay = new Dictionary<string, string>();
                                TMap _map81 = await protocol.ReadMapBeginAsync(cancellationToken);
                                for (int _i82 = 0; _i82 < _map81.Count; ++_i82)
                                {
                                    string _key83;
                                    string _val84;
                                    _key83 = await protocol.ReadStringAsync(cancellationToken);
                                    _val84 = await protocol.ReadStringAsync(cancellationToken);
                                    ConfOverlay[_key83] = _val84;
                                }
                                await protocol.ReadMapEndAsync(cancellationToken);
                            }
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 4:
                        if (field.Type == TType.Bool)
                        {
                            RunAsync = await protocol.ReadBoolAsync(cancellationToken);
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
            if (!isset_sessionHandle)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_statement)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TExecuteStatementReq");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField {Name = "sessionHandle", Type = TType.Struct, ID = 1};
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await SessionHandle.WriteAsync(protocol);
            await protocol.WriteFieldEndAsync(cancellationToken);
            field.Name = "statement";
            field.Type = TType.String;
            field.ID = 2;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteStringAsync(Statement, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            if (ConfOverlay != null && _isSet.confOverlay)
            {
                field.Name = "confOverlay";
                field.Type = TType.Map;
                field.ID = 3;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                {
                    await protocol.WriteMapBeginAsync(new TMap(TType.String, TType.String, ConfOverlay.Count), cancellationToken);
                    foreach (string key in ConfOverlay.Keys)
                    {
                        await protocol.WriteStringAsync(key, cancellationToken);
                        await protocol.WriteStringAsync(ConfOverlay[key], cancellationToken);
                    }
                    await protocol.WriteMapEndAsync(cancellationToken);
                }
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (_isSet.runAsync)
            {
                field.Name = "runAsync";
                field.Type = TType.Bool;
                field.ID = 4;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await protocol.WriteBoolAsync(RunAsync, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TExecuteStatementReq(");
            sb.Append("SessionHandle: ");
            sb.Append(SessionHandle == null ? "<null>" : SessionHandle.ToString());
            sb.Append(",Statement: ");
            sb.Append(Statement);
            sb.Append(",ConfOverlay: ");
            sb.Append(ConfOverlay);
            sb.Append(",RunAsync: ");
            sb.Append(RunAsync);
            sb.Append(")");
            return sb.ToString();
        }
    }
}
