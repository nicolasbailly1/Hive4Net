using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Datasets
{
    public abstract class ColumnValueBase<T> : TBase
    {
        protected abstract string ColumnName { get; }
        protected abstract TType TType { get; }
        protected abstract Func<TProtocol, CancellationToken, Task<T>> FuncRead { get; }
        protected abstract Func<TProtocol, T, CancellationToken, Task> FuncWrite { get; }

        private T _value;

        public T Value
        {
            get
            {
                return _value;
            }
            set
            {
                _isSet.value = true;
                this._value = value;
            }
        }

        public Isset _isSet;

        public struct Isset
        {
            public bool value;
        }

        protected ColumnValueBase()
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
                        if (field.Type == TType)
                        {
                            Value = await FuncRead(protocol, cancellationToken);
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
            TStruct struc = new TStruct(ColumnName);
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            if (_isSet.value)
            {
                field.Name = "value";
                field.Type = TType;
                field.ID = 1;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await FuncWrite(protocol, Value, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder($"{ColumnName}(");
            sb.Append("Value: ");
            sb.Append(Value);
            sb.Append(")");
            return sb.ToString();
        }
    }

    public class BoolValue : ColumnValueBase<bool>
    {
        protected override string ColumnName => "TBoolValue";
        protected override TType TType => TType.Bool;
        protected override Func<TProtocol, CancellationToken, Task<bool>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadBoolAsync(cancellationToken);
        protected override Func<TProtocol, bool, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteBoolAsync(element, cancellationToken);
    }

    public class I16Value : ColumnValueBase<short>
    {
        protected override string ColumnName => "TI16Value";
        protected override TType TType => TType.I16;
        protected override Func<TProtocol, CancellationToken, Task<short>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadI16Async(cancellationToken);
        protected override Func<TProtocol, short, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteI16Async(element, cancellationToken);
    }

    public class I32Value : ColumnValueBase<int>
    {
        protected override string ColumnName => "TI32Value";
        protected override TType TType => TType.I32;
        protected override Func<TProtocol, CancellationToken, Task<int>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadI32Async(cancellationToken);
        protected override Func<TProtocol, int, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteI32Async(element, cancellationToken);
    }

    public class I64Value : ColumnValueBase<long>
    {
        protected override string ColumnName => "TI64Value";
        protected override TType TType => TType.I64;
        protected override Func<TProtocol, CancellationToken, Task<long>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadI64Async(cancellationToken);
        protected override Func<TProtocol, long, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteI64Async(element, cancellationToken);
    }

    public class DoubleValue : ColumnValueBase<double>
    {
        protected override string ColumnName => "TDoubleValue";
        protected override TType TType => TType.Double;
        protected override Func<TProtocol, CancellationToken, Task<double>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadDoubleAsync(cancellationToken);
        protected override Func<TProtocol, double, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteDoubleAsync(element, cancellationToken);
    }

    public class StringValue : ColumnValueBase<string>
    {
        protected override string ColumnName => "TStringValue";
        protected override TType TType => TType.String;
        protected override Func<TProtocol, CancellationToken, Task<string>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadStringAsync(cancellationToken);
        protected override Func<TProtocol, string, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteStringAsync(element, cancellationToken);
    }

    public class ByteValue : ColumnValueBase<sbyte>
    {
        protected override string ColumnName => "TByteValue";
        protected override TType TType => TType.Byte;
        protected override Func<TProtocol, CancellationToken, Task<sbyte>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadByteAsync(cancellationToken);
        protected override Func<TProtocol, sbyte, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteByteAsync(element, cancellationToken);
    }

    public class BinaryValue : ColumnValueBase<byte[]>
    {
        protected override string ColumnName => "TBinaryValue";
        protected override TType TType => TType.Byte;
        protected override Func<TProtocol, CancellationToken, Task<byte[]>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadBinaryAsync(cancellationToken);
        protected override Func<TProtocol, byte[], CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteBinaryAsync(element, cancellationToken);
    }
}
