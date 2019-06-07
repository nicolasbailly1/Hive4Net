using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    public partial class RowSet : TBase
    {
        private List<Column> _columns;

        public long StartRowOffset { get; set; }

        public List<Row> Rows { get; set; }

        public List<Column> Columns
        {
            get
            {
                return _columns;
            }
            set
            {
                _isSet.columns = true;
                this._columns = value;
            }
        }

        public Isset _isSet;

        public struct Isset
        {
            public bool columns;
        }

        public RowSet()
        {
        }

        public RowSet(long startRowOffset, List<Row> rows) : this()
        {
            this.StartRowOffset = startRowOffset;
            this.Rows = rows;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_startRowOffset = false;
            bool isset_rows = false;
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
                        if (field.Type == TType.I64)
                        {
                            StartRowOffset = await protocol.ReadI64Async(cancellationToken);
                            isset_startRowOffset = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 2:
                        if (field.Type == TType.List)
                        {
                            {
                                Rows = new List<Row>();
                                TList _list59 = await protocol.ReadListBeginAsync(cancellationToken);
                                for (int _i60 = 0; _i60 < _list59.Count; ++_i60)
                                {
                                    Row _elem61 = new Row();
                                    await _elem61.ReadAsync(protocol, cancellationToken);
                                    Rows.Add(_elem61);
                                }
                                await protocol.ReadListEndAsync(cancellationToken);
                            }
                            isset_rows = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 3:
                        if (field.Type == TType.List)
                        {
                            {
                                Columns = new List<Column>();
                                TList _list62 = await protocol.ReadListBeginAsync(cancellationToken);
                                for (int _i63 = 0; _i63 < _list62.Count; ++_i63)
                                {
                                    Column _elem64 = new Column();
                                    await _elem64.ReadAsync(protocol, cancellationToken);
                                    Columns.Add(_elem64);
                                }
                                await protocol.ReadListEndAsync(cancellationToken);
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
            if (!isset_startRowOffset)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_rows)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TRowSet");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            field.Name = "startRowOffset";
            field.Type = TType.I64;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteI64Async(StartRowOffset, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            field.Name = "rows";
            field.Type = TType.List;
            field.ID = 2;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            {
                await protocol.WriteListBeginAsync(new TList(TType.Struct, Rows.Count), cancellationToken);
                foreach (Row _iter65 in Rows)
                {
                   await _iter65.WriteAsync(protocol, cancellationToken);
                }
                await protocol.WriteListEndAsync(cancellationToken);
            }
            await protocol.WriteFieldEndAsync(cancellationToken);
            if (Columns != null && _isSet.columns)
            {
                field.Name = "columns";
                field.Type = TType.List;
                field.ID = 3;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                {
                    await protocol.WriteListBeginAsync(new TList(TType.Struct, Columns.Count), cancellationToken);
                    foreach (Column _iter66 in Columns)
                    {
                        await _iter66.WriteAsync(protocol, cancellationToken);
                    }
                    await protocol.WriteListEndAsync(cancellationToken);
                }
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TRowSet(");
            sb.Append("StartRowOffset: ");
            sb.Append(StartRowOffset);
            sb.Append(",Rows: ");
            sb.Append(Rows);
            sb.Append(",Columns: ");
            sb.Append(Columns);
            sb.Append(")");
            return sb.ToString();
        }
    }
}
