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
    public partial class TypeDesc : TBase
    {

        public List<TypeEntry> Types { get; set; }

        public TypeDesc()
        {
        }

        public TypeDesc(List<TypeEntry> types) : this()
        {
            this.Types = types;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_types = false;
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
                                Types = new List<TypeEntry>();
                                TList _list15 = await protocol.ReadListBeginAsync(cancellationToken);
                                for (int _i16 = 0; _i16 < _list15.Count; ++_i16)
                                {
                                    TypeEntry _elem17 = new TypeEntry();
                                    _elem17 = new TypeEntry();
                                    await _elem17.ReadAsync(protocol, cancellationToken);
                                    Types.Add(_elem17);
                                }
                                await protocol.ReadListEndAsync(cancellationToken);
                            }
                            isset_types = true;
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
            if (!isset_types)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TTypeDesc");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            field.Name = "types";
            field.Type = TType.List;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            {
                await protocol.WriteListBeginAsync(new TList(TType.Struct, Types.Count), cancellationToken);
                foreach (TypeEntry _iter18 in Types)
                {
                    await _iter18.WriteAsync(protocol, cancellationToken);
                }
                await protocol.WriteListEndAsync(cancellationToken);
            }
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TTypeDesc(");
            sb.Append("Types: ");
            sb.Append(Types);
            sb.Append(")");
            return sb.ToString();
        }

    }
}
