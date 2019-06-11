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
    public partial class TypeEntry : TBase
    {
        private PrimitiveTypeEntry _primitiveEntry;
        private ArrayTypeEntry _arrayEntry;
        private MapTypeEntry _mapEntry;
        private StructTypeEntry _structEntry;
        private UnionTypeEntry _unionEntry;
        private UserDefinedTypeEntry _userDefinedTypeEntry;

        public PrimitiveTypeEntry PrimitiveEntry
        {
            get => _primitiveEntry;
            set
            {
                _isSet.primitiveEntry = true;
                this._primitiveEntry = value;
            }
        }

        public ArrayTypeEntry ArrayEntry
        {
            get => _arrayEntry;
            set
            {
                _isSet.arrayEntry = true;
                this._arrayEntry = value;
            }
        }

        public MapTypeEntry MapEntry
        {
            get => _mapEntry;
            set
            {
                _isSet.mapEntry = true;
                this._mapEntry = value;
            }
        }

        public StructTypeEntry StructEntry
        {
            get => _structEntry;
            set
            {
                _isSet.structEntry = true;
                this._structEntry = value;
            }
        }

        public UnionTypeEntry UnionEntry
        {
            get => _unionEntry;
            set
            {
                _isSet.unionEntry = true;
                this._unionEntry = value;
            }
        }

        public UserDefinedTypeEntry UserDefinedTypeEntry
        {
            get => _userDefinedTypeEntry;
            set
            {
                _isSet.userDefinedTypeEntry = true;
                this._userDefinedTypeEntry = value;
            }
        }


        public Isset _isSet;
        [Serializable]
        public struct Isset
        {
            public bool primitiveEntry;
            public bool arrayEntry;
            public bool mapEntry;
            public bool structEntry;
            public bool unionEntry;
            public bool userDefinedTypeEntry;
        }

        public TypeEntry()
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
                            PrimitiveEntry = new PrimitiveTypeEntry();
                            await PrimitiveEntry.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;
                    case 2:
                        if (field.Type == TType.Struct)
                        {
                            ArrayEntry = new ArrayTypeEntry();
                            await ArrayEntry.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;
                    case 3:
                        if (field.Type == TType.Struct)
                        {
                            MapEntry = new MapTypeEntry();
                            await MapEntry.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;
                    case 4:
                        if (field.Type == TType.Struct)
                        {
                            StructEntry = new StructTypeEntry();
                            await StructEntry.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;
                    case 5:
                        if (field.Type == TType.Struct)
                        {
                            UnionEntry = new UnionTypeEntry();
                            await UnionEntry.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;
                    case 6:
                        if (field.Type == TType.Struct)
                        {
                            UserDefinedTypeEntry = new UserDefinedTypeEntry();
                            await UserDefinedTypeEntry.ReadAsync(protocol, cancellationToken);
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
            TStruct struc = new TStruct("TTypeEntry");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            if (PrimitiveEntry != null && _isSet.primitiveEntry)
            {
                field.Name = "primitiveEntry";
                field.Type = TType.Struct;
                field.ID = 1;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await PrimitiveEntry.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (ArrayEntry != null && _isSet.arrayEntry)
            {
                field.Name = "arrayEntry";
                field.Type = TType.Struct;
                field.ID = 2;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await ArrayEntry.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (MapEntry != null && _isSet.mapEntry)
            {
                field.Name = "mapEntry";
                field.Type = TType.Struct;
                field.ID = 3;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await MapEntry.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (StructEntry != null && _isSet.structEntry)
            {
                field.Name = "structEntry";
                field.Type = TType.Struct;
                field.ID = 4;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await StructEntry.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (UnionEntry != null && _isSet.unionEntry)
            {
                field.Name = "unionEntry";
                field.Type = TType.Struct;
                field.ID = 5;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await UnionEntry.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (UserDefinedTypeEntry != null && _isSet.userDefinedTypeEntry)
            {
                field.Name = "userDefinedTypeEntry";
                field.Type = TType.Struct;
                field.ID = 6;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await UserDefinedTypeEntry.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TTypeEntry(");
            sb.Append("PrimitiveEntry: ");
            sb.Append(PrimitiveEntry == null ? "<null>" : PrimitiveEntry.ToString());
            sb.Append(",ArrayEntry: ");
            sb.Append(ArrayEntry == null ? "<null>" : ArrayEntry.ToString());
            sb.Append(",MapEntry: ");
            sb.Append(MapEntry == null ? "<null>" : MapEntry.ToString());
            sb.Append(",StructEntry: ");
            sb.Append(StructEntry == null ? "<null>" : StructEntry.ToString());
            sb.Append(",UnionEntry: ");
            sb.Append(UnionEntry == null ? "<null>" : UnionEntry.ToString());
            sb.Append(",UserDefinedTypeEntry: ");
            sb.Append(UserDefinedTypeEntry == null ? "<null>" : UserDefinedTypeEntry.ToString());
            sb.Append(")");
            return sb.ToString();
        }

    }
}
