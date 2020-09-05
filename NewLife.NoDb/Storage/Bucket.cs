using System;
using System.Collections.Generic;

namespace NewLife.NoDb.Storage
{
    internal class Bucket
    {
        /// <summary>数据桶集合</summary>
        internal readonly DbNode[] Buckets;

        internal readonly Object[] Locks;

        internal volatile Int32[] CountPerLock;

        internal readonly IEqualityComparer<Byte[]> Comparer;

        internal Bucket(DbNode[] buckets, Object[] locks, Int32[] countPerLock, IEqualityComparer<Byte[]> comparer)
        {
            Buckets = buckets;
            Locks = locks;
            CountPerLock = countPerLock;
            Comparer = comparer;
        }
    }
}