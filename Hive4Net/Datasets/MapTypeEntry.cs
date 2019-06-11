using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    public partial class MapTypeEntry : TBase
    {
        public int KeyTypePtr { get; set; }

        public int ValueTypePtr { get; set; }

        public MapTypeEntry()
        {
        }

        public MapTypeEntry(int keyTypePtr, int valueTypePtr) : this()
        {
            this.KeyTypePtr = keyTypePtr;
            this.ValueTypePtr = valueTypePtr;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_keyTypePtr = false;
            bool isset_valueTypePtr = false;
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
                        if (field.Type == TType.I32)
                        {
                            KeyTypePtr = await protocol.ReadI32Async(cancellationToken);
                            isset_keyTypePtr = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 2:
                        if (field.Type == TType.I32)
                        {
                            ValueTypePtr = await protocol.ReadI32Async(cancellationToken);
                            isset_valueTypePtr = true;
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
            if (!isset_keyTypePtr)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_valueTypePtr)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TMapTypeEntry");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField {Name = "keyTypePtr", Type = TType.I32, ID = 1};
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteI32Async(KeyTypePtr, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            field.Name = "valueTypePtr";
            field.Type = TType.I32;
            field.ID = 2;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteI32Async(ValueTypePtr, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TMapTypeEntry(");
            sb.Append("KeyTypePtr: ");
            sb.Append(KeyTypePtr);
            sb.Append(",ValueTypePtr: ");
            sb.Append(ValueTypePtr);
            sb.Append(")");
            return sb.ToString();
        }
    }
}
