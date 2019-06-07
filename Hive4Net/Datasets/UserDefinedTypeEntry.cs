using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    public partial class UserDefinedTypeEntry : TBase
    {
        public string TypeClassName { get; set; }

        public UserDefinedTypeEntry()
        {
        }

        public UserDefinedTypeEntry(string typeClassName) : this()
        {
            this.TypeClassName = typeClassName;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_typeClassName = false;
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
                            TypeClassName = await protocol.ReadStringAsync(cancellationToken);
                            isset_typeClassName = true;
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
            if (!isset_typeClassName)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TUserDefinedTypeEntry");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            field.Name = "typeClassName";
            field.Type = TType.String;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteStringAsync(TypeClassName, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TUserDefinedTypeEntry(");
            sb.Append("TypeClassName: ");
            sb.Append(TypeClassName);
            sb.Append(")");
            return sb.ToString();
        }
    }
}
