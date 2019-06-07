using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hive4Net.enums;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.Requests
{
    public class OpenSessionRequest : TBase
    {
        private string _username;
        private string _password;
        private Dictionary<string, string> _configuration;

        /// <summary>
        ///
        /// <seealso cref="TProtocolVersion"/>
        /// </summary>
        public ProtocolVersion Client_protocol { get; set; }

        public string Username
        {
            get
            {
                return _username;
            }
            set
            {
                _isSet.username = true;
                this._username = value;
            }
        }

        public string Password
        {
            get
            {
                return _password;
            }
            set
            {
                _isSet.password = true;
                this._password = value;
            }
        }

        public Dictionary<string, string> Configuration
        {
            get
            {
                return _configuration;
            }
            set
            {
                _isSet.configuration = true;
                this._configuration = value;
            }
        }

        public Isset _isSet;

        public struct Isset
        {
            public bool username;
            public bool password;
            public bool configuration;
        }

        public OpenSessionRequest()
        {
            this.Client_protocol = ProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6;
        }

        public OpenSessionRequest(ProtocolVersion client_protocol) : this()
        {
            this.Client_protocol = client_protocol;
        }

        public async Task ReadAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            bool isset_client_protocol = false;
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
                            Client_protocol = (ProtocolVersion)await protocol.ReadI32Async(cancellationToken);
                            isset_client_protocol = true;
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 2:
                        if (field.Type == TType.String)
                        {
                            Username = await protocol.ReadStringAsync(cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 3:
                        if (field.Type == TType.String)
                        {
                            Password = await protocol.ReadStringAsync(cancellationToken);
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, cancellationToken);
                        }
                        break;

                    case 4:
                        if (field.Type == TType.Map)
                        {
                            {
                                Configuration = new Dictionary<string, string>();
                                TMap _map71 = await protocol.ReadMapBeginAsync(cancellationToken);
                                for (int _i72 = 0; _i72 < _map71.Count; ++_i72)
                                {
                                    string _key73;
                                    string _val74;
                                    _key73 = await protocol.ReadStringAsync(cancellationToken);
                                    _val74 = await protocol.ReadStringAsync(cancellationToken);
                                    Configuration[_key73] = _val74;
                                }
                                await protocol.ReadMapEndAsync(cancellationToken);
                            }
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
            if (!isset_client_protocol)
                throw new TProtocolException(TProtocolException.INVALID_DATA);
        }

        public async Task WriteAsync(TProtocol protocol, CancellationToken cancellationToken)
        {
            TStruct struc = new TStruct("TOpenSessionReq");
            await protocol.WriteStructBeginAsync(struc, cancellationToken);
            TField field = new TField();
            field.Name = "client_protocol";
            field.Type = TType.I32;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field, cancellationToken);
            await protocol.WriteI32Async((int)Client_protocol, cancellationToken);
            await protocol.WriteFieldEndAsync(cancellationToken);
            if (Username != null && _isSet.username)
            {
                field.Name = "username";
                field.Type = TType.String;
                field.ID = 2;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await protocol.WriteStringAsync(Username, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (Password != null && _isSet.password)
            {
                field.Name = "password";
                field.Type = TType.String;
                field.ID = 3;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                await protocol.WriteStringAsync(Password, cancellationToken);
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            if (Configuration != null && _isSet.configuration)
            {
                field.Name = "configuration";
                field.Type = TType.Map;
                field.ID = 4;
                await protocol.WriteFieldBeginAsync(field, cancellationToken);
                {
                    await protocol.WriteMapBeginAsync(new TMap(TType.String, TType.String, Configuration.Count), cancellationToken);
                    foreach (string _iter75 in Configuration.Keys)
                    {
                        await protocol.WriteStringAsync(_iter75, cancellationToken);
                        await protocol.WriteStringAsync(Configuration[_iter75], cancellationToken);
                    }
                    await protocol.WriteMapEndAsync(cancellationToken);
                }
                await protocol.WriteFieldEndAsync(cancellationToken);
            }
            await protocol.WriteFieldStopAsync(cancellationToken);
            await protocol.WriteStructEndAsync(cancellationToken);
        }
    }
}
