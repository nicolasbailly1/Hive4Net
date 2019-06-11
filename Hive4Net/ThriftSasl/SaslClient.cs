using System;

namespace Hive4Net.ThriftSasl
{
    public class SaslClient : IDisposable
    {
        PlainMechanism _choseMechanism;

        public SaslClient(string host, PlainMechanism mechanism)
        {
            this.Mechanism = mechanism.Name;
            _choseMechanism = mechanism;

        }

        public string Mechanism
        {
            get;
            private set;
        }

        public byte[] process(byte[] challenge)
        {
            return _choseMechanism.Process(challenge);
        }

        public void Dispose()
        {
            _choseMechanism = null;
        }
    }
}
