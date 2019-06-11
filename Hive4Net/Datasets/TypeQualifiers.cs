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
    public partial class TypeQualifiers : TBase
    {

        public Dictionary<string, TypeQualifierValue> Qualifiers { get; set; }

        public TypeQualifiers()
        {
        }

        public TypeQualifiers(Dictionary<string, TypeQualifierValue> qualifiers) : this()
        {
            this.Qualifiers = qualifiers;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_qualifiers = false;
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
                        if (field.Type == TType.Map)
                        {
                            Qualifiers = new Dictionary<string, TypeQualifierValue>();
                            TMap map = await protocol.ReadMapBeginAsync(cancellationToken);
                            for (int i = 0; i < map.Count; ++i)
                            {
                                string key;
                                TypeQualifierValue val;
                                key = await protocol.ReadStringAsync(cancellationToken);
                                val = new TypeQualifierValue();
                                await val.ReadAsync(protocol, cancellationToken);
                                Qualifiers[key] = val;
                            }
                            await protocol.ReadMapEndAsync(cancellationToken);
                            isset_qualifiers = true;
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
            if (!isset_qualifiers)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TTypeQualifiers");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField {Name = "qualifiers", Type = TType.Map, ID = 1};
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            {
                await protocol.WriteMapBeginAsync(new TMap(TType.String, TType.Struct, Qualifiers.Count), cancellationToken);
                foreach (string _iter4 in Qualifiers.Keys)
                {
                    await protocol.WriteStringAsync(_iter4, cancellationToken);
                    await Qualifiers[_iter4].WriteAsync(protocol, cancellationToken);
                }
                await protocol.WriteMapEndAsync(cancellationToken);
            }
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TTypeQualifiers(");
            sb.Append("Qualifiers: ");
            sb.Append(Qualifiers);
            sb.Append(")");
            return sb.ToString();
        }

    }
}
