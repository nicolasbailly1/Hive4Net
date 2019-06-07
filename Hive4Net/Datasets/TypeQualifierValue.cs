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
    public partial class TypeQualifierValue : TBase
    {
        private int _i32Value;
        private string _stringValue;

        public int I32Value
        {
            get
            {
                return _i32Value;
            }
            set
            {
                _isSet.i32Value = true;
                this._i32Value = value;
            }
        }

        public string StringValue
        {
            get
            {
                return _stringValue;
            }
            set
            {
                _isSet.stringValue = true;
                this._stringValue = value;
            }
        }


        public Isset _isSet;
        [Serializable]
        public struct Isset
        {
            public bool i32Value;
            public bool stringValue;
        }

        public TypeQualifierValue()
        {
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            
            await protocol.ReadStructBeginAsync(cancellationToken);
            TField field = await protocol.ReadFieldBeginAsync(cancellationToken);
            while (field.Type != TType.Stop)
            {
                switch (field.ID)
                {
                    case 1:
                        if (field.Type == TType.I32)
                        {
                            I32Value = await protocol.ReadI32Async(cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;
                    case 2:
                        if (field.Type == TType.String)
                        {
                            StringValue = await protocol.ReadStringAsync(cancellationToken);
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
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TTypeQualifierValue");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            if (_isSet.i32Value)
            {
                field.Name = "i32Value";
                field.Type = TType.I32;
                field.ID = 1;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await protocol.WriteI32Async(I32Value, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (StringValue != null && _isSet.stringValue)
            {
                field.Name = "stringValue";
                field.Type = TType.String;
                field.ID = 2;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await protocol.WriteStringAsync(StringValue, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TTypeQualifierValue(");
            sb.Append("I32Value: ");
            sb.Append(I32Value);
            sb.Append(",StringValue: ");
            sb.Append(StringValue);
            sb.Append(")");
            return sb.ToString();
        }

    }
}
