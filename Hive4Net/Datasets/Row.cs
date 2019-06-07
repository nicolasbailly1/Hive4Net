using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    public partial class Row : TBase
    {
        public List<ColumnValue> ColVals { get; set; }

        public Row()
        {
        }

        public Row(List<ColumnValue> colVals) : this()
        {
            this.ColVals = colVals;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_colVals = false;
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
                        if (field.Type == TType.List)
                        {
                            {
                                ColVals = new List<ColumnValue>();
                                TList _list23 = await protocol.ReadListBeginAsync(cancellationToken);
                                for (int _i24 = 0; _i24 < _list23.Count; ++_i24)
                                {
                                    ColumnValue _elem25 = new ColumnValue();
                                    _elem25 = new ColumnValue();
                                    await _elem25.ReadAsync(protocol, cancellationToken);
                                    ColVals.Add(_elem25);
                                }
                                await protocol.ReadListEndAsync(cancellationToken);
                            }
                            isset_colVals = true;
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
            if (!isset_colVals)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TRow");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            field.Name = "colVals";
            field.Type = TType.List;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            {
                await protocol.WriteListBeginAsync(new TList(TType.Struct, ColVals.Count), cancellationToken);
                foreach (ColumnValue _iter26 in ColVals)
                {
                    await _iter26.WriteAsync(protocol, cancellationToken);
                }
                await protocol.WriteListEndAsync(cancellationToken);
            }
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TRow(");
            sb.Append("ColVals: ");
            sb.Append(ColVals);
            sb.Append(")");
            return sb.ToString();
        }
    }
}
