using System;

namespace NewLife.NoDb.Storage
{
    internal class DbNode
    {
        internal Byte[] Key;

        internal Block Value;

        internal volatile DbNode Next;

        internal Int32 HashCode;

        internal DbNode(Byte[] key, Block value, Int32 hashcode, DbNode next)
        {
            Key = key;
            Value = value;
            Next = next;
            HashCode = hashcode;
        }

        public Boolean IsKey(Byte[] key)
        {
            if (key == Key) return true;
            if (key.Length != Key.Length) return false;

            for (var i = 0; i < key.Length; i++)
            {
                if (key[i] != Key[i]) return false;
            }

            return true;
        }
    }
}