using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    [Serializable]
    public partial class TableSchema : TBase
    {

        public List<ColumnDesc> Columns { get; set; }

        public TableSchema()
        {
        }

        public TableSchema(List<ColumnDesc> columns) : this()
        {
            this.Columns = columns;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_columns = false;
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
                                Columns = new List<ColumnDesc>();
                                TList list = await protocol.ReadListBeginAsync(cancellationToken);
                                for (int i = 0; i < list.Count; ++i)
                                {
                                    ColumnDesc desc = new ColumnDesc();
                                    await desc.ReadAsync(protocol, cancellationToken);
                                    Columns.Add(desc);
                                }
                                await protocol.ReadListEndAsync(cancellationToken);
                            }
                            isset_columns = true;
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
            if (!isset_columns)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TTableSchema");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField {Name = "columns", Type = TType.List, ID = 1};
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            {
                await protocol.WriteListBeginAsync(new TList(TType.Struct, Columns.Count), cancellationToken);
                foreach (ColumnDesc desc in Columns)
                {
                    await desc.WriteAsync(protocol, cancellationToken);
                }
                await protocol.WriteListEndAsync(cancellationToken);
            }
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TTableSchema(");
            sb.Append("Columns: ");
            sb.Append(Columns);
            sb.Append(")");
            return sb.ToString();
        }

    }
}
