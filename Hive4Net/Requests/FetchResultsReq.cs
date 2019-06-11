using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.enums;
using Hive4Net.TCliService;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Requests
{
    public partial class FetchResultsReq : TBase
    {
        public OperationHandle OperationHandle { get; set; }

        /// <summary>
        ///
        /// <seealso cref="FetchOrientation"/>
        /// </summary>
        public FetchOrientation Orientation { get; set; }

        public long MaxRows { get; set; }

        public FetchResultsReq()
        {
            this.Orientation = FetchOrientation.FETCH_NEXT;
        }

        public FetchResultsReq(OperationHandle operationHandle, FetchOrientation orientation, long maxRows) : this()
        {
            this.OperationHandle = operationHandle;
            this.Orientation = orientation;
            this.MaxRows = maxRows;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_operationHandle = false;
            bool isset_orientation = false;
            bool isset_maxRows = false;
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

                    case 2:
                        if (field.Type == TType.I32)
                        {
                            Orientation = (FetchOrientation)await protocol.ReadI32Async(cancellationToken);
                            isset_orientation = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 3:
                        if (field.Type == TType.I64)
                        {
                            MaxRows = await protocol.ReadI64Async(cancellationToken);
                            isset_maxRows = true;
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
            if (!isset_orientation)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_maxRows)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TFetchResultsReq");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField {Name = "operationHandle", Type = TType.Struct, ID = 1};
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await OperationHandle.WriteAsync(protocol);
            await protocol.WriteFieldEndAsync(cancellationToken);
            field.Name = "orientation";
            field.Type = TType.I32;
            field.ID = 2;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteI32Async((int)Orientation, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            field.Name = "maxRows";
            field.Type = TType.I64;
            field.ID = 3;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteI64Async(MaxRows, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TFetchResultsReq(");
            sb.Append("OperationHandle: ");
            sb.Append(OperationHandle == null ? "<null>" : OperationHandle.ToString());
            sb.Append(",Orientation: ");
            sb.Append(Orientation);
            sb.Append(",MaxRows: ");
            sb.Append(MaxRows);
            sb.Append(")");
            return sb.ToString();
        }
    }
}
