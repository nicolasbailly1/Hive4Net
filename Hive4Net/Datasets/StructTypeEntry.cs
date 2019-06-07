using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    public partial class StructTypeEntry : TBase
    {
        public Dictionary<string, int> NameToTypePtr { get; set; }

        public StructTypeEntry()
        {
        }

        public StructTypeEntry(Dictionary<string, int> nameToTypePtr) : this()
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
                            TMap _map5 = await protocol.ReadMapBeginAsync(cancellationToken);
                            for (int _i6 = 0; _i6 < _map5.Count; ++_i6)
                            {
                                string _key7;
                                int _val8;
                                _key7 = await protocol.ReadStringAsync(cancellationToken);
                                _val8 = await protocol.ReadI32Async(cancellationToken);
                                NameToTypePtr[_key7] = _val8;
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
            TStruct struc = new TStruct("TStructTypeEntry");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            field.Name = "nameToTypePtr";
            field.Type = TType.Map;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            {
                await protocol.WriteMapBeginAsync(new TMap(TType.String, TType.I32, NameToTypePtr.Count), cancellationToken);
                foreach (string _iter9 in NameToTypePtr.Keys)
                {
                    await protocol.WriteStringAsync(_iter9, cancellationToken);
                    await protocol.WriteI32Async(NameToTypePtr[_iter9], cancellationToken);
                }
                await protocol.WriteMapEndAsync(cancellationToken);
            }
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TStructTypeEntry(");
            sb.Append("NameToTypePtr: ");
            sb.Append(NameToTypePtr);
            sb.Append(")");
            return sb.ToString();
        }
    }
}
