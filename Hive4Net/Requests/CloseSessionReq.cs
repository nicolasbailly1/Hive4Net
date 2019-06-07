using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.TCliService;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Requests
{
    public partial class CloseSessionReq : TBase
    {
        public SessionHandle SessionHandle { get; set; }

        public CloseSessionReq()
        {
        }

        public CloseSessionReq(SessionHandle sessionHandle) : this()
        {
            this.SessionHandle = sessionHandle;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_sessionHandle = false;
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

                    default:
                        await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        break;
                }
                await protocol.ReadFieldEndAsync(cancellationToken);
            }
            await protocol.ReadStructEndAsync(cancellationToken);
            if (!isset_sessionHandle)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TCloseSessionReq");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            field.Name = "sessionHandle";
            field.Type = TType.Struct;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await SessionHandle.WriteAsync(protocol);
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TCloseSessionReq(");
            sb.Append("SessionHandle: ");
            sb.Append(SessionHandle == null ? "<null>" : SessionHandle.ToString());
            sb.Append(")");
            return sb.ToString();
        }
    }
}
