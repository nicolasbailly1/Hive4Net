using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.Requests;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    public partial class FetchResultsArgs : TBase
    {
        private FetchResultsReq _req;

        public FetchResultsReq Req
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

        public FetchResultsArgs()
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
                            Req = new FetchResultsReq();
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
            TStruct struc = new TStruct("FetchResults_args");
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
            StringBuilder sb = new StringBuilder("FetchResults_args(");
            sb.Append("Req: ");
            sb.Append(Req == null ? "<null>" : Req.ToString());
            sb.Append(")");
            return sb.ToString();
        }
    }
}
