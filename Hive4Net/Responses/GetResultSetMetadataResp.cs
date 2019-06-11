using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.Datasets;
using Hive4Net.TCliService;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Responses
{
    public partial class GetResultSetMetadataResp : TBase
    {
        private TableSchema _schema;

        public Status Status { get; set; }

        public TableSchema Schema
        {
            get => _schema;
            set
            {
                _isSet.schema = true;
                this._schema = value;
            }
        }

        public Isset _isSet;

        public struct Isset
        {
            public bool schema;
        }

        public GetResultSetMetadataResp()
        {
        }

        public GetResultSetMetadataResp(Status status) : this()
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

                    case 2:
                        if (field.Type == TType.Struct)
                        {
                            Schema = new TableSchema();
                            await Schema.ReadAsync(protocol, cancellationToken);
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
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TGetResultSetMetadataResp");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField {Name = "status", Type = TType.Struct, ID = 1};
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await Status.WriteAsync(protocol);
            await protocol.WriteFieldEndAsync(cancellationToken);
            if (Schema != null && _isSet.schema)
            {
                field.Name = "schema";
                field.Type = TType.Struct;
                field.ID = 2;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await Schema.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TGetResultSetMetadataResp(");
            sb.Append("Status: ");
            sb.Append(Status == null ? "<null>" : Status.ToString());
            sb.Append(",Schema: ");
            sb.Append(Schema == null ? "<null>" : Schema.ToString());
            sb.Append(")");
            return sb.ToString();
        }
    }
}
