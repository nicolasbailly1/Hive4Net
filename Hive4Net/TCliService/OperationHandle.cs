using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.enums;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.TCliService
{
    public class OperationHandle
    {
        private double _modifiedRowCount;

        public HandleIdentifier OperationId { get; set; }

        /// <summary>
        /// 
        /// <seealso cref="OperationType"/>
        /// </summary>
        public OperationType OperationType { get; set; }

        public bool HasResultSet { get; set; }

        public double ModifiedRowCount
        {
            get => _modifiedRowCount;
            set
            {
                __isset.modifiedRowCount = true;
                this._modifiedRowCount = value;
            }
        }


        public Isset __isset;

        [Serializable]
        public struct Isset
        {
            public bool modifiedRowCount;
        }

        public OperationHandle()
        {
        }

        public OperationHandle(HandleIdentifier operationId, OperationType operationType, bool hasResultSet) : this()
        {
            this.OperationId = operationId;
            this.OperationType = operationType;
            this.HasResultSet = hasResultSet;
        }

        public async Task ReadAsync(TProtocol protocol)
        {
            bool isset_operationId = false;
            bool isset_operationType = false;
            bool isset_hasResultSet = false;
            TField field;
            await protocol.ReadStructBeginAsync();
            while (true)
            {
                field = await protocol.ReadFieldBeginAsync();
                if (field.Type == TType.Stop)
                {
                    break;
                }
                switch (field.ID)
                {
                    case 1:
                        if (field.Type == TType.Struct)
                        {
                            OperationId = new HandleIdentifier();
                            await OperationId.ReadAsync(protocol);
                            isset_operationId = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                        }
                        break;
                    case 2:
                        if (field.Type == TType.I32)
                        {
                            OperationType = (OperationType)await protocol.ReadI32Async();
                            isset_operationType = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                        }
                        break;
                    case 3:
                        if (field.Type == TType.Bool)
                        {
                            HasResultSet = await protocol.ReadBoolAsync();
                            isset_hasResultSet = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                        }
                        break;
                    case 4:
                        if (field.Type == TType.Double)
                        {
                            ModifiedRowCount = await protocol.ReadDoubleAsync();
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                        }
                        break;
                    default:
                        await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                        break;
                }
                await protocol.ReadFieldEndAsync();
            }
            await protocol.ReadStructEndAsync();
            if (!isset_operationId)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_operationType)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
            if (!isset_hasResultSet)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol)
        {
            TStruct struc = new TStruct("TOperationHandle");
            await protocol.WriteStructBeginAsync(struc);
            TField field = new TField();
            field.Name = "operationId";
            field.Type = TType.Struct;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field);
            await OperationId.WriteAsync(protocol);
            await protocol.WriteFieldEndAsync();
            field.Name = "operationType";
            field.Type = TType.I32;
            field.ID = 2;
            await protocol.WriteFieldBeginAsync(field);
            await protocol.WriteI32Async((int)OperationType);
            await protocol.WriteFieldEndAsync();
            field.Name = "hasResultSet";
            field.Type = TType.Bool;
            field.ID = 3;
            await protocol.WriteFieldBeginAsync(field);
            await protocol.WriteBoolAsync(HasResultSet);
            await protocol.WriteFieldEndAsync();
            if (__isset.modifiedRowCount)
            {
                field.Name = "modifiedRowCount";
                field.Type = TType.Double;
                field.ID = 4;
                await protocol.WriteFieldBeginAsync(field);
                await protocol.WriteDoubleAsync(ModifiedRowCount);
                await protocol.WriteFieldEndAsync();
            }
            await protocol.WriteFieldStopAsync();
            await protocol.WriteStructEndAsync();
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TOperationHandle(");
            sb.Append("OperationId: ");
            sb.Append(OperationId == null ? "<null>" : OperationId.ToString());
            sb.Append(",OperationType: ");
            sb.Append(OperationType);
            sb.Append(",HasResultSet: ");
            sb.Append(HasResultSet);
            sb.Append(",ModifiedRowCount: ");
            sb.Append(ModifiedRowCount);
            sb.Append(")");
            return sb.ToString();
        }
    }
}
