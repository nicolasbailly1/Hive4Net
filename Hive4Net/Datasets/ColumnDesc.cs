using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    [Serializable]
    public partial class ColumnDesc : TBase
    {
        private string _comment;

        public string ColumnName { get; set; }

        public TypeDesc TypeDesc { get; set; }

        public int Position { get; set; }

        public string Comment
        {
            get => _comment;
            set
            {
                _isSet.comment = true;
                this._comment = value;
            }
        }


        public Isset _isSet;
        [Serializable]
        public struct Isset
        {
            public bool comment;
        }

        public ColumnDesc()
        {
        }

        public ColumnDesc(string columnName, TypeDesc typeDesc, int position) : this()
        {
            this.ColumnName = columnName;
            this.TypeDesc = typeDesc;
            this.Position = position;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_columnName = false;
            bool isset_typeDesc = false;
            bool isset_position = false;
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
                        if (field.Type == TType.String)
                        {
                            ColumnName = await protocol.ReadStringAsync(cancellationToken);
                            isset_columnName = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;
                    case 2:
                        if (field.Type == TType.Struct)
                        {
                            TypeDesc = new TypeDesc();
                            await TypeDesc.ReadAsync(protocol, cancellationToken);
                            isset_typeDesc = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;
                    case 3:
                        if (field.Type == TType.I32)
                        {
                            Position = await protocol.ReadI32Async(cancellationToken);
                            isset_position = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;
                    case 4:
                        if (field.Type == TType.String)
                        {
                            Comment = await protocol.ReadStringAsync(cancellationToken);
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
            if (!isset_columnName)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_typeDesc)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_position)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TColumnDesc");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField {Name = "columnName", Type = TType.String, ID = 1};
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteStringAsync(ColumnName, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            field.Name = "typeDesc";
            field.Type = TType.Struct;
            field.ID = 2;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await TypeDesc.WriteAsync(protocol, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            field.Name = "position";
            field.Type = TType.I32;
            field.ID = 3;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteI32Async(Position, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            if (Comment != null && _isSet.comment)
            {
                field.Name = "comment";
                field.Type = TType.String;
                field.ID = 4;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await protocol.WriteStringAsync(Comment, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TColumnDesc(");
            sb.Append("ColumnName: ");
            sb.Append(ColumnName);
            sb.Append(",TypeDesc: ");
            sb.Append(TypeDesc == null ? "<null>" : TypeDesc.ToString());
            sb.Append(",Position: ");
            sb.Append(Position);
            sb.Append(",Comment: ");
            sb.Append(Comment);
            sb.Append(")");
            return sb.ToString();
        }

    }
}
