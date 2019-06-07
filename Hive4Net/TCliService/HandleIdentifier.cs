using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.TCliService
{
    public class HandleIdentifier
    {
        public byte[] Guid { get; set; }
        public byte[] Secret { get; set; }
        public async Task ReadAsync(Thrift.Protocols.TProtocol protocol)
        {
            await protocol.ReadStructBeginAsync();
            var field = await protocol.ReadFieldBeginAsync();
            while (field.Type != TType.Stop)
            {
                switch (field.ID)
                {
                    case 1:
                        if (field.Type == TType.String)
                        {
                            Guid = await protocol.ReadBinaryAsync();
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                        }
                        break;
                    case 2:
                        if (field.Type == TType.String)
                        {
                            Secret = await protocol.ReadBinaryAsync();
                        }
                        else
                        {
                            await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                        }
                        break;
                }

                await protocol.ReadFieldEndAsync();
                field = await protocol.ReadFieldBeginAsync();
            }

            await protocol.ReadStructEndAsync();
        }

        public async Task WriteAsync(Thrift.Protocols.TProtocol protocol)
        {
            TStruct struc = new TStruct("THandleIdentifier");
            await protocol.WriteStructBeginAsync(struc);
            TField field = new TField();
            field.Name = "guid";
            field.Type = TType.String;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field);
            await protocol.WriteBinaryAsync(Guid);
            await protocol.WriteFieldEndAsync();
            field.Name = "secret";
            field.Type = TType.String;
            field.ID = 2;
            await protocol.WriteFieldBeginAsync(field);
            await protocol.WriteBinaryAsync(Secret);
            await protocol.WriteFieldEndAsync();
            await protocol.WriteFieldStopAsync();
            await protocol.WriteStructEndAsync();
        }
    }
}
