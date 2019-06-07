using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.TCliService;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Responses
{
    public partial class CloseSessionResp : TBase
    {
        public Status Status { get; set; }

        public CloseSessionResp()
        {
        }

        public CloseSessionResp(Status status) : this()
        {
            this.Status = status;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_status = false;
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

                    default:
                        await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        break;
                }
                await protocol.ReadFieldBeginAsync(cancellationToken);
            }
            await protocol.ReadStructEndAsync(cancellationToken);
            if (!isset_status)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TCloseSessionResp");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            field.Name = "status";
            field.Type = TType.Struct;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await Status.WriteAsync(protocol);
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TCloseSessionResp(");
            sb.Append("Status: ");
            sb.Append(Status == null ? "<null>" : Status.ToString());
            sb.Append(")");
            return sb.ToString();
        }
    }
}
