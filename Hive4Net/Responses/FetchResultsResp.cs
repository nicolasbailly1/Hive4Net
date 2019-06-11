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
    public partial class FetchResultsResp : TBase
    {
        private bool _hasMoreRows;
        private RowSet _results;

        public Status Status { get; set; }

        public bool HasMoreRows
        {
            get => _hasMoreRows;
            set
            {
                _isSet.hasMoreRows = true;
                this._hasMoreRows = value;
            }
        }

        public RowSet Results
        {
            get => _results;
            set
            {
                _isSet.results = true;
                this._results = value;
            }
        }

        public Isset _isSet;

        public struct Isset
        {
            public bool hasMoreRows;
            public bool results;
        }

        public FetchResultsResp()
        {
        }

        public FetchResultsResp(Status status) : this()
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
                        if (field.Type == TType.Bool)
                        {
                            HasMoreRows = await protocol.ReadBoolAsync(cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 3:
                        if (field.Type == TType.Struct)
                        {
                            Results = new RowSet();
                            await Results.ReadAsync(protocol, cancellationToken);
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
            TStruct struc = new TStruct("TFetchResultsResp");
            await protocol.WriteStructBeginAsync(struc, CancellationToken.None);
            TField field = new TField {Name = "status", Type = TType.Struct, ID = 1};
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await Status.WriteAsync(protocol);
            await protocol.WriteFieldEndAsync(cancellationToken);
            if (_isSet.hasMoreRows)
            {
                field.Name = "hasMoreRows";
                field.Type = TType.Bool;
                field.ID = 2;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await protocol.WriteBoolAsync(HasMoreRows, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (Results != null && _isSet.results)
            {
                field.Name = "results";
                field.Type = TType.Struct;
                field.ID = 3;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await Results.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TFetchResultsResp(");
            sb.Append("Status: ");
            sb.Append(Status == null ? "<null>" : Status.ToString());
            sb.Append(",HasMoreRows: ");
            sb.Append(HasMoreRows);
            sb.Append(",Results: ");
            sb.Append(Results == null ? "<null>" : Results.ToString());
            sb.Append(")");
            return sb.ToString();
        }
    }
}
