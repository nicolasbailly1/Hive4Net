using System.Collections;

namespace Hive4Net
{
    public static class Utils
    {
        public static bool IsEmpty(this IEnumerable enumerable)
        {
            var enumerator = enumerable != null ? enumerable.GetEnumerator() : null;
            return enumerator == null || !enumerator.MoveNext();
        }

        public static void EncodeBigEndian(int integer, byte[] buf, int offset)
        {
            buf[offset] = (byte)(0xff & (integer >> 24));
            buf[offset + 1] = (byte)(0xff & (integer >> 16));
            buf[offset + 2] = (byte)(0xff & (integer >> 8));
            buf[offset + 3] = (byte)(0xff & (integer));
        }

        public static int DecodeBigEndian(byte[] buf)
        {
            return DecodeBigEndian(buf, 0);
        }

        public static int DecodeBigEndian(byte[] buf, int offset)
        {
            return ((buf[offset] & 0xff) << 24) | ((buf[offset + 1] & 0xff) << 16)
                                                | ((buf[offset + 2] & 0xff) << 8) | ((buf[offset + 3] & 0xff));
        }

        public static void EncodeFrameSize(int frameSize, byte[] buf)
        {
            buf[0] = (byte)(0xff & (frameSize >> 24));
            buf[1] = (byte)(0xff & (frameSize >> 16));
            buf[2] = (byte)(0xff & (frameSize >> 8));
            buf[3] = (byte)(0xff & (frameSize));
        }

        public static int DecodeFrameSize(byte[] buf)
        {
            return
                ((buf[0] & 0xff) << 24) |
                ((buf[1] & 0xff) << 16) |
                ((buf[2] & 0xff) << 8) |
                ((buf[3] & 0xff));
        }
    }
}
