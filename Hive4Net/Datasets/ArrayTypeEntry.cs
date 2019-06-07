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
    public partial class ArrayTypeEntry : TBase
    {

        public int ObjectTypePtr { get; set; }

        public ArrayTypeEntry()
        {
        }

        public ArrayTypeEntry(int objectTypePtr) : this()
        {
            this.ObjectTypePtr = objectTypePtr;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_objectTypePtr = false;
            await protocol.ReadStructBeginAsync(cancellationToken);
            TField field = await protocol.ReadFieldBeginAsync(cancellationToken);
            while (field.Type != TType.Stop)
            {
                switch (field.ID)
                {
                    case 1:
                        if (field.Type == TType.I32)
                        {
                            ObjectTypePtr = await protocol.ReadI32Async(cancellationToken);
                            isset_objectTypePtr = true;
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

                field = await protocol.ReadFieldBeginAsync(cancellationToken);
            }
            await protocol.ReadStructEndAsync(cancellationToken);
            if (!isset_objectTypePtr)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TArrayTypeEntry");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            field.Name = "objectTypePtr";
            field.Type = TType.I32;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteI32Async(ObjectTypePtr, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TArrayTypeEntry(");
            sb.Append("ObjectTypePtr: ");
            sb.Append(ObjectTypePtr);
            sb.Append(")");
            return sb.ToString();
        }

    }
}
