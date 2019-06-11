using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    public partial class UnionTypeEntry : TBase
    {
        public Dictionary<string, int> NameToTypePtr { get; set; }

        public UnionTypeEntry()
        {
        }

        public UnionTypeEntry(Dictionary<string, int> nameToTypePtr) : this()
        {
            this.NameToTypePtr = nameToTypePtr;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_nameToTypePtr = false;
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
                            NameToTypePtr = new Dictionary<string, int>();
                            TMap map = await protocol.ReadMapBeginAsync(cancellationToken);
                            for (int i = 0; i < map.Count; ++i)
                            {
                                var key = await protocol.ReadStringAsync(cancellationToken);
                                var val = await protocol.ReadI32Async(cancellationToken);
                                NameToTypePtr[key] = val;
                            }
                            await protocol.ReadMapEndAsync(cancellationToken);
                            isset_nameToTypePtr = true;
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
            if (!isset_nameToTypePtr)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TUnionTypeEntry");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField {Name = "nameToTypePtr", Type = TType.Map, ID = 1};
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteMapBeginAsync(new TMap(TType.String, TType.I32, NameToTypePtr.Count), cancellationToken);
            foreach (string key in NameToTypePtr.Keys)
            {
                await protocol.WriteStringAsync(key, cancellationToken);
                await protocol.WriteI32Async(NameToTypePtr[key], cancellationToken);
            }
            await protocol.WriteMapEndAsync(cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TUnionTypeEntry(");
            sb.Append("NameToTypePtr: ");
            sb.Append(NameToTypePtr);
            sb.Append(")");
            return sb.ToString();
        }
    }
}
