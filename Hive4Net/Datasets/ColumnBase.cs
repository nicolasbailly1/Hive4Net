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
    public abstract class ColumnBase<T> : TBase
    {
        protected abstract string ColumnName { get; }
        protected abstract TType TType { get; }
        protected abstract Func<TProtocol, CancellationToken, Task<T>> FuncRead { get; }
        protected abstract Func<TProtocol, T, CancellationToken, Task> FuncWrite { get; }
        
        public List<T> Values { get; set; }

        public byte[] Nulls { get; set; }

        protected ColumnBase()
        {
        }

        protected ColumnBase(List<T> values, byte[] nulls)
        {
            this.Values = values;
            this.Nulls = nulls;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_values = false;
            bool isset_nulls = false;
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
                        if (field.Type == TType.List)
                        {
                            {
                                Values = new List<T>();
                                TList _list39 = await protocol.ReadListBeginAsync(cancellationToken);
                                for (int _i40 = 0; _i40 < _list39.Count; ++_i40)
                                {
                                    T _elem41 = default(T);
                                    _elem41 = await FuncRead(protocol, cancellationToken);
                                    Values.Add(_elem41);
                                }
                                await protocol.ReadListEndAsync(cancellationToken);
                            }
                            isset_values = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 2:
                        if (field.Type == TType.String)
                        {
                            Nulls = await protocol.ReadBinaryAsync(cancellationToken);
                            isset_nulls = true;
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
            if (!isset_values)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_nulls)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct(ColumnName);
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            field.Name = "values";
            field.Type = TType.List;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteListBeginAsync(new TList(TType, Values.Count), cancellationToken);
            foreach (T _iter42 in Values)
            {
                await FuncWrite(protocol, _iter42, cancellationToken);
            }
            await protocol.WriteListEndAsync(cancellationToken);
            
            await protocol.WriteFieldEndAsync(cancellationToken);
            field.Name = "nulls";
            field.Type = TType.String;
            field.ID = 2;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteBinaryAsync(Nulls, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder($"{ColumnName}(");
            sb.Append("Values: ");
            sb.Append(Values);
            sb.Append(",Nulls: ");
            sb.Append(Nulls);
            sb.Append(")");
            return sb.ToString();
        }
        
    }

    public class BoolColumn : ColumnBase<bool>
    {
        protected override string ColumnName => "TBoolColumn";
        protected override TType TType => TType.Bool;
        protected override Func<TProtocol, CancellationToken, Task<bool>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadBoolAsync(cancellationToken);
        protected override Func<TProtocol, bool, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteBoolAsync(element, cancellationToken);
    }

    public class I16Column : ColumnBase<short>
    {
        protected override string ColumnName => "TI16Column";
        protected override TType TType => TType.I16;
        protected override Func<TProtocol, CancellationToken, Task<short>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadI16Async(cancellationToken);
        protected override Func<TProtocol, short, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteI16Async(element, cancellationToken);
    }

    public class I32Column : ColumnBase<int>
    {
        protected override string ColumnName => "TI32Column";
        protected override TType TType => TType.I32;
        protected override Func<TProtocol, CancellationToken, Task<int>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadI32Async(cancellationToken);
        protected override Func<TProtocol, int, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteI32Async(element, cancellationToken);
    }

    public class I64Column : ColumnBase<long>
    {
        protected override string ColumnName => "TI64Column";
        protected override TType TType => TType.I64;
        protected override Func<TProtocol, CancellationToken, Task<long>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadI64Async(cancellationToken);
        protected override Func<TProtocol, long, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteI64Async(element, cancellationToken);
    }

    public class DoubleColumn : ColumnBase<double>
    {
        protected override string ColumnName => "TDoubleColumn";
        protected override TType TType => TType.Double;
        protected override Func<TProtocol, CancellationToken, Task<double>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadDoubleAsync(cancellationToken);
        protected override Func<TProtocol, double, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteDoubleAsync(element, cancellationToken);
    }

    public class StringColumn : ColumnBase<string>
    {
        protected override string ColumnName => "TStringColumn";
        protected override TType TType => TType.String;
        protected override Func<TProtocol, CancellationToken, Task<string>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadStringAsync(cancellationToken);
        protected override Func<TProtocol, string, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteStringAsync(element, cancellationToken);
    }

    public class ByteColumn : ColumnBase<sbyte>
    {
        protected override string ColumnName => "TByteColumn";
        protected override TType TType => TType.Byte;
        protected override Func<TProtocol, CancellationToken, Task<sbyte>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadByteAsync(cancellationToken);
        protected override Func<TProtocol, sbyte, CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteByteAsync(element, cancellationToken);
    }

    public class BinaryColumn : ColumnBase<byte[]>
    {
        protected override string ColumnName => "TBinaryColumn";
        protected override TType TType => TType.Byte;
        protected override Func<TProtocol, CancellationToken, Task<byte[]>> FuncRead => async (protocol, cancellationToken) => await protocol.ReadBinaryAsync(cancellationToken);
        protected override Func<TProtocol, byte[], CancellationToken, Task> FuncWrite => async (protocol, element, cancellationToken) => await protocol.WriteBinaryAsync(element, cancellationToken);
    }
}
