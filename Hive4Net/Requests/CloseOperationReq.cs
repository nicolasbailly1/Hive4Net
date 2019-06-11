using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.TCliService;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Requests
{
    public partial class CloseOperationReq : TBase
    {
        public OperationHandle OperationHandle { get; set; }

        public CloseOperationReq()
        {
        }

        public CloseOperationReq(OperationHandle operationHandle) : this()
        {
            this.OperationHandle = operationHandle;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_operationHandle = false;
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
                            OperationHandle = new OperationHandle();
                            await OperationHandle.ReadAsync(protocol);
                            isset_operationHandle = true;
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
            if (!isset_operationHandle)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TCloseOperationReq");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField {Name = "operationHandle", Type = TType.Struct, ID = 1};
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await OperationHandle.WriteAsync(protocol);
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TCloseOperationReq(");
            sb.Append("OperationHandle: ");
            sb.Append(OperationHandle == null ? "<null>" : OperationHandle.ToString());
            sb.Append(")");
            return sb.ToString();
        }
    }
}
