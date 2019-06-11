using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.enums;
using Hive4Net.TCliService;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Responses
{
    public class OpenSessionResp : TBase
    {
        private SessionHandle _sessionHandle;
        private Dictionary<string, string> _configuration;

        public Status Status { get; set; }

        public ProtocolVersion ServerProtocolVersion { get; set; }

        public SessionHandle SessionHandle
        {
            get => _sessionHandle;
            set
            {
                _isSet.sessionHandle = true;
                this._sessionHandle = value;
            }
        }

        public Dictionary<string, string> Configuration
        {
            get => _configuration;
            set
            {
                _isSet.configuration = true;
                this._configuration = value;
            }
        }

        public Isset _isSet;

        public struct Isset
        {
            public bool sessionHandle;
            public bool configuration;
        }

        public OpenSessionResp()
        {
            this.ServerProtocolVersion = ProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6;
        }

        public OpenSessionResp(Status status, ProtocolVersion serverProtocolVersion) : this()
        {
            this.Status = status;
            this.ServerProtocolVersion = serverProtocolVersion;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_status = false;
            bool isset_serverProtocolVersion = false;
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
                            Status = new Status();
                            await Status.ReadAsync(protocol);
                            isset_status = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 2:
                        if (field.Type == TType.I32)
                        {
                            ServerProtocolVersion = (ProtocolVersion)await protocol.ReadI32Async(cancellationToken);
                            isset_serverProtocolVersion = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 3:
                        if (field.Type == TType.Struct)
                        {
                            SessionHandle = new SessionHandle();
                            await SessionHandle.ReadAsync(protocol);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 4:
                        if (field.Type == TType.Map)
                        {
                            {
                                Configuration = new Dictionary<string, string>();
                                TMap map = await protocol.ReadMapBeginAsync(cancellationToken);
                                for (int i = 0; i < map.Count; ++i)
                                {
                                    var key = await protocol.ReadStringAsync(cancellationToken);
                                    var val = await protocol.ReadStringAsync(cancellationToken);
                                    Configuration[key] = val;
                                }
                                await protocol.ReadMapEndAsync(cancellationToken);
                            }
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
            if (!isset_status)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_serverProtocolVersion)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TOpenSessionResp");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField {Name = "status", Type = TType.Struct, ID = 1};
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await Status.WriteAsync(protocol);
            await protocol.WriteFieldEndAsync(cancellationToken);
            field.Name = "serverProtocolVersion";
            field.Type = TType.I32;
            field.ID = 2;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteI32Async((int)ServerProtocolVersion, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            if (SessionHandle != null && _isSet.sessionHandle)
            {
                field.Name = "sessionHandle";
                field.Type = TType.Struct;
                field.ID = 3;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await SessionHandle.WriteAsync(protocol);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (Configuration != null && _isSet.configuration)
            {
                field.Name = "configuration";
                field.Type = TType.Map;
                field.ID = 4;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                {
                    await protocol.WriteMapBeginAsync(new TMap(TType.String, TType.String, Configuration.Count), cancellationToken);
                    foreach (string key in Configuration.Keys)
                    {
                        await protocol.WriteStringAsync(key, cancellationToken);
                        await protocol.WriteStringAsync(Configuration[key], cancellationToken);
                    }
                    await protocol.WriteMapEndAsync(cancellationToken);
                }
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TOpenSessionResp(");
            sb.Append("Status: ");
            sb.Append(Status == null ? "<null>" : Status.ToString());
            sb.Append(",ServerProtocolVersion: ");
            sb.Append(ServerProtocolVersion);
            sb.Append(",SessionHandle: ");
            sb.Append(SessionHandle == null ? "<null>" : SessionHandle.ToString());
            sb.Append(",Configuration: ");
            sb.Append(Configuration);
            sb.Append(")");
            return sb.ToString();
        }
    }
}
