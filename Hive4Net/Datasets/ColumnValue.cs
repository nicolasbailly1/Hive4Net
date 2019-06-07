using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    public partial class ColumnValue : TBase
    {
        private BoolValue _boolVal;
        private ByteValue _byteVal;
        private I16Value _i16Val;
        private I32Value _i32Val;
        private I64Value _i64Val;
        private DoubleValue _doubleVal;
        private StringValue _stringVal;

        public BoolValue BoolVal
        {
            get => _boolVal;
            set
            {
                _isSet.boolVal = true;
                this._boolVal = value;
            }
        }

        public ByteValue ByteVal
        {
            get => _byteVal;
            set
            {
                _isSet.byteVal = true;
                this._byteVal = value;
            }
        }

        public I16Value I16Val
        {
            get => _i16Val;
            set
            {
                _isSet.i16Val = true;
                this._i16Val = value;
            }
        }

        public I32Value I32Val
        {
            get => _i32Val;
            set
            {
                _isSet.i32Val = true;
                this._i32Val = value;
            }
        }

        public I64Value I64Val
        {
            get => _i64Val;
            set
            {
                _isSet.i64Val = true;
                this._i64Val = value;
            }
        }

        public DoubleValue DoubleVal
        {
            get => _doubleVal;
            set
            {
                _isSet.doubleVal = true;
                this._doubleVal = value;
            }
        }

        public StringValue StringVal
        {
            get => _stringVal;
            set
            {
                _isSet.stringVal = true;
                this._stringVal = value;
            }
        }

        public Isset _isSet;

        public struct Isset
        {
            public bool boolVal;
            public bool byteVal;
            public bool i16Val;
            public bool i32Val;
            public bool i64Val;
            public bool doubleVal;
            public bool stringVal;
        }

        public ColumnValue()
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
                            BoolVal = new BoolValue();
                            await BoolVal.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 2:
                        if (field.Type == TType.Struct)
                        {
                            ByteVal = new ByteValue();
                            await ByteVal.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 3:
                        if (field.Type == TType.Struct)
                        {
                            I16Val = new I16Value();
                            await I16Val.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 4:
                        if (field.Type == TType.Struct)
                        {
                            I32Val = new I32Value();
                            await I32Val.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 5:
                        if (field.Type == TType.Struct)
                        {
                            I64Val = new I64Value();
                            await I64Val.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 6:
                        if (field.Type == TType.Struct)
                        {
                            DoubleVal = new DoubleValue();
                            await DoubleVal.ReadAsync(protocol, cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 7:
                        if (field.Type == TType.Struct)
                        {
                            StringVal = new StringValue();
                            await StringVal.ReadAsync(protocol, cancellationToken);
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
            TStruct struc = new TStruct("TColumnValue");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            if (BoolVal != null && _isSet.boolVal)
            {
                field.Name = "boolVal";
                field.Type = TType.Struct;
                field.ID = 1;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await BoolVal.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (ByteVal != null && _isSet.byteVal)
            {
                field.Name = "byteVal";
                field.Type = TType.Struct;
                field.ID = 2;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await ByteVal.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (I16Val != null && _isSet.i16Val)
            {
                field.Name = "i16Val";
                field.Type = TType.Struct;
                field.ID = 3;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await I16Val.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (I32Val != null && _isSet.i32Val)
            {
                field.Name = "i32Val";
                field.Type = TType.Struct;
                field.ID = 4;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await I32Val.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (I64Val != null && _isSet.i64Val)
            {
                field.Name = "i64Val";
                field.Type = TType.Struct;
                field.ID = 5;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await I64Val.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (DoubleVal != null && _isSet.doubleVal)
            {
                field.Name = "doubleVal";
                field.Type = TType.Struct;
                field.ID = 6;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await DoubleVal.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (StringVal != null && _isSet.stringVal)
            {
                field.Name = "stringVal";
                field.Type = TType.Struct;
                field.ID = 7;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await StringVal.WriteAsync(protocol, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TColumnValue(");
            sb.Append("BoolVal: ");
            sb.Append(BoolVal == null ? "<null>" : BoolVal.ToString());
            sb.Append(",ByteVal: ");
            sb.Append(ByteVal == null ? "<null>" : ByteVal.ToString());
            sb.Append(",I16Val: ");
            sb.Append(I16Val == null ? "<null>" : I16Val.ToString());
            sb.Append(",I32Val: ");
            sb.Append(I32Val == null ? "<null>" : I32Val.ToString());
            sb.Append(",I64Val: ");
            sb.Append(I64Val == null ? "<null>" : I64Val.ToString());
            sb.Append(",DoubleVal: ");
            sb.Append(DoubleVal == null ? "<null>" : DoubleVal.ToString());
            sb.Append(",StringVal: ");
            sb.Append(StringVal == null ? "<null>" : StringVal.ToString());
            sb.Append(")");
            return sb.ToString();
        }
    }
}
