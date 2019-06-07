using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Hive4Net.TCliService
{
    public class SessionHandle
    {
        public HandleIdentifier SessionId { get; set; }
        public async Task ReadAsync(Thrift.Protocols.TProtocol protocol)
        {
            await protocol.ReadStructBeginAsync();
            var field = await protocol.ReadFieldBeginAsync();
            while (field.Type != TType.Stop)
            {
                if (field.ID == 1)
                {
                    if (field.Type == TType.Struct)
                    {
                        SessionId = new HandleIdentifier();
                        await SessionId.ReadAsync(protocol);
                    }
                    else
                    {
                        await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                    }
                }
                else
                {
                    await TProtocolUtil.SkipAsync(protocol, field.Type, CancellationToken.None);
                }
                await protocol.ReadFieldEndAsync();
                field = await protocol.ReadFieldBeginAsync();
            }

            await protocol.ReadStructEndAsync();
        }

        public async Task WriteAsync(TProtocol protocol)
        {
            TStruct struc = new TStruct("TSessionHandle");
            await protocol.WriteStructBeginAsync(struc);
            TField field = new TField();
            field.Name = "sessionId";
            field.Type = TType.Struct;
            field.ID = 1;
            await protocol.WriteFieldBeginAsync(field);
            await SessionId.WriteAsync(protocol);
            await protocol.WriteFieldEndAsync();
            await protocol.WriteFieldStopAsync();
            await protocol.WriteStructEndAsync();
        }
    }
}
