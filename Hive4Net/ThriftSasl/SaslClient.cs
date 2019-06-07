using System;

namespace Hive4Net.ThriftSasl
{
    public class SaslClient : IDisposable
    {
        PlainMechanism _chose_mechanism;

        public SaslClient(string host, PlainMechanism mechanism)
        {
            this.Mechanism = mechanism.Name;
            _chose_mechanism = mechanism;

        }

        public string Mechanism
        {
            get;
            private set;
        }

        public byte[] process(byte[] challenge)
        {
            return _chose_mechanism.process(challenge);
        }

        public void Dispose()
        {
            _chose_mechanism = null;
        }
    }
}
