using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.enums;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.TCliService
{
    public class Status
    {
        private List<string> _infoMessages;
        private string _sqlState;
        private int _errorCode;
        private string _errorMessage;

        public StatusCode StatusCode { get; set; }

        public List<string> InfoMessages
        {
            get => _infoMessages;
            set
            {
                _isSet.infoMessages = true;
                this._infoMessages = value;
            }
        }

        public string SqlState
        {
            get => _sqlState;
            set
            {
                _isSet.sqlState = true;
                this._sqlState = value;
            }
        }

        public int ErrorCode
        {
            get => _errorCode;
            set
            {
                _isSet.errorCode = true;
                this._errorCode = value;
            }
        }

        public string ErrorMessage
        {
            get => _errorMessage;
            set
            {
                _isSet.errorMessage = true;
                this._errorMessage = value;
            }
        }

        public Isset _isSet;

        public struct Isset
        {
            public bool infoMessages;
            public bool sqlState;
            public bool errorCode;
            public bool errorMessage;
        }

        public async Task ReadAsync(Thrift.Protocols.TProtocol protocol)
        {
            await protocol.ReadStructBeginAsync();
            var field = await protocol.ReadFieldBeginAsync();
            while (field.Type != TType.Stop)
            {
                switch (field.ID)
                {
                    case 1:
                        if (field.Type == TType.I32)
                        {
                            StatusCode = (StatusCode)await protocol.ReadI32Async();
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                        }

                        break;
                    case 2:
                        if (field.Type == TType.List)
                        {
                            InfoMessages = new List<string>();
                            var list = await protocol.ReadListBeginAsync();

                            for (int i = 0; i < list.Count; i++)
                            {
                                string element = await protocol.ReadStringAsync();
                                InfoMessages.Add(element);
                            }

                            await protocol.ReadListEndAsync();
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                        }

                        break;
                    case 3:
                        if (field.Type == TType.String)
                        {
                            SqlState = await protocol.ReadStringAsync();
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                        }

                        break;
                    case 4:
                        if (field.Type == TType.I32)
                        {
                            ErrorCode = await protocol.ReadI32Async();
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                        }

                        break;
                    case 5:
                        if (field.Type == TType.String)
                        {
                            ErrorMessage = await protocol.ReadStringAsync();
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

                field = await protocol.ReadFieldBeginAsync();
            }

            await protocol.ReadStructEndAsync();
        }

        public async Task WriteAsync(TProtocol protocol)
        {
            TStruct struc = new TStruct("TStatus");
            await protocol.WriteStructBeginAsync(struc);
            TField field = new TField {Name = "statusCode", Type = TType.I32, ID = 1};
            await protocol.WriteFieldBeginAsync(field);
            await protocol.WriteI32Async((int)StatusCode);
            await protocol.WriteFieldEndAsync();
            if (InfoMessages != null && _isSet.infoMessages)
            {
                field.Name = "infoMessages";
                field.Type = TType.List;
                field.ID = 2;
                await protocol.WriteFieldBeginAsync(field);
                {
                    await protocol.WriteListBeginAsync(new TList(TType.String, InfoMessages.Count));
                    foreach (string message in InfoMessages)
                    {
                        await protocol.WriteStringAsync(message);
                    }
                    await protocol.WriteListEndAsync();
                }
                await protocol.WriteFieldEndAsync();
            }
            if (SqlState != null && _isSet.sqlState)
            {
                field.Name = "sqlState";
                field.Type = TType.String;
                field.ID = 3;
                await protocol.WriteFieldBeginAsync(field);
                await protocol.WriteStringAsync(SqlState);
                await protocol.WriteFieldEndAsync();
            }
            if (_isSet.errorCode)
            {
                field.Name = "errorCode";
                field.Type = TType.I32;
                field.ID = 4;
                await protocol.WriteFieldBeginAsync(field);
                await protocol.WriteI32Async(ErrorCode);
                await protocol.WriteFieldEndAsync();
            }
            if (ErrorMessage != null && _isSet.errorMessage)
            {
                field.Name = "errorMessage";
                field.Type = TType.String;
                field.ID = 5;
                await protocol.WriteFieldBeginAsync(field);
                await protocol.WriteStringAsync(ErrorMessage);
                await protocol.WriteFieldEndAsync();
            }
            await protocol.WriteFieldStopAsync();
            await protocol.WriteStructEndAsync();
        }

        public void CheckStatus()
        {
            if (_isSet.errorMessage)
            {
                throw new Exception(ErrorMessage);
            }
        }
    }
}
