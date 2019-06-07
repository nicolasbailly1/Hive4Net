using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.enums;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    [Serializable]
    public partial class PrimitiveTypeEntry : TBase
    {
        private TypeQualifiers _typeQualifiers;

        /// <summary>
        /// 
        /// <seealso cref="TTypeId"/>
        /// </summary>
        public TypeId Type { get; set; }

        public TypeQualifiers TypeQualifiers
        {
            get
            {
                return _typeQualifiers;
            }
            set
            {
                _isSet.typeQualifiers = true;
                this._typeQualifiers = value;
            }
        }


        public Isset _isSet;
        [Serializable]
        public struct Isset
        {
            public bool typeQualifiers;
        }

        public PrimitiveTypeEntry()
        {
        }

        public PrimitiveTypeEntry(TypeId type) : this()
        {
            this.Type = type;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_type = false;
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
                            Type = (TypeId)await protocol.ReadI32Async(cancellationToken);
                            isset_type = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;
                    case 2:
                        if (field.Type == TType.Struct)
                        {
                            TypeQualifiers = new TypeQualifiers();
                            await TypeQualifiers.ReadAsync(protocol, cancellationToken);
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
            if (!isset_type)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TPrimitiveTypeEntry");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            field.Name = "type";
            field.Type = TType.I32;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteI32Async((int)Type, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            if (TypeQualifiers != null && _isSet.typeQualifiers)
            {
                field.Name = "typeQualifiers";
                field.Type = TType.Struct;
                field.ID = 2;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await TypeQualifiers.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TPrimitiveTypeEntry(");
            sb.Append("Type: ");
            sb.Append(Type);
            sb.Append(",TypeQualifiers: ");
            sb.Append(TypeQualifiers == null ? "<null>" : TypeQualifiers.ToString());
            sb.Append(")");
            return sb.ToString();
        }

    }
}
