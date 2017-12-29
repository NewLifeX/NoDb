using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;

namespace NewLife.NoDb.Storage
{
    /// <summary>索引</summary>
    /// <remarks>
    /// 根据哈希分为很多个数据桶，桶内数据采用链表存储，指向数据区。
    /// 索引区分为头部、哈希桶、链表数据三部分。
    /// 
    /// </remarks>
    class DbIndex
    {
        #region 属性
        private readonly MemoryMappedFile _mmf;
        private readonly Block _block;

        private UnmanagedMemoryAccessor _head;
        private UnmanagedMemoryAccessor _buckets;
        private UnmanagedMemoryAccessor _data;

        private volatile Bucket _table;
        private Int32 _keyRehashCount;
        private Int32 _budget;
        #endregion

        #region 构造
        public DbIndex(MemoryMappedFile mmf, Block block)
        {
            if (block.IsNull) throw new ArgumentNullException(nameof(block));

            _mmf = mmf ?? throw new ArgumentNullException(nameof(mmf));
            _block = block;

            // 加载失败后，重建索引区
            if (!Load(mmf, block)) Save(mmf, block);
        }
        #endregion

        #region 高级属性
        /// <summary>总记录数</summary>
        public Int32 Count { get { return _head.ReadInt32(0); } private set { _head.Write(0, value); } }

        /// <summary>桶个数。质数</summary>
        public Int32 BucketCount { get { return _head.ReadInt32(4); } private set { _head.Write(4, value); } }

        /// <summary>装载因子。总记录数除以哈希表大小，以0.75为宜</summary>
        public Double LoadingFactor { get { return BucketCount == 0 ? 0 : (Double)Count / BucketCount; } }

        /// <summary>获取或设置与指定的键关联的值</summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public Block this[Byte[] key]
        {
            get
            {
                if (!TryGetValue(key, out var result)) throw new KeyNotFoundException();

                return result;
            }
            set
            {
                if (key == null) throw new ArgumentNullException(nameof(key));

                TryAddInternal(key, value, true, true, out var val);
            }
        }
        #endregion

        #region 基础读写
        //private MemoryMappedViewAccessor GetAccessor()
        //{
        //    var accessor = _mmf.CreateAccessor(_block);
        //    return accessor;
        //}

        private Boolean Load(MemoryMappedFile mmf, Block block)
        {
            if (block.Size < 256) return false;

            // 索引区结构：头部 + 哈希桶 + 链表数据
            var ac = mmf.CreateAccessor(block);

            // 前面256字节为头部
            var buf = ac.ReadArray(0, 256);
            var ms = new MemoryStream(buf);
            var reader = new BinaryReader(ms);

            // 头部
            var bk2 = new Block(reader.ReadInt32(), reader.ReadInt32());
            var bk3 = new Block(reader.ReadInt32(), reader.ReadInt32());
            var len = (Int32)ms.Position;
            var crc = reader.ReadUInt32();

            // 校验数据
            if (crc != buf.ReadBytes(0, len).Crc()) return false;

            // 头部
            _head = mmf.CreateViewAccessor(block.Position + ms.Position, ms.Length - ms.Position);

            // 哈希桶
            _buckets = mmf.CreateAccessor(bk2 + block.Position);

            // 链表数据
            _data = mmf.CreateAccessor(bk3 + block.Position);

            return true;
        }

        private void Save(MemoryMappedFile mmf, Block block)
        {
            if (block.Size < 256) throw new ArgumentOutOfRangeException(nameof(block));
            //// 最小数
            //var min = MinSize;
            //if (block.Size < min) block.Size = min;

            // 索引区结构：头部 + 哈希桶 + 链表数据

            // 前面256字节为头部
            var buf = new Byte[256];
            var ms = new MemoryStream(buf);

            // 头部
            var bk1 = new Block(8 * 5, 256 - 8 * 5);
            var bk2 = new Block(bk1.Position + bk1.Size, 31 * 16);
            var bk3 = new Block(bk2.Position + bk2.Size, 1024 * 1024);

            var writer = new BinaryWriter(ms);
            writer.Write((Int32)bk2.Position);
            writer.Write((Int32)bk2.Size);
            writer.Write((Int32)bk3.Position);
            writer.Write((Int32)bk3.Size);

            // 校验
            var crc = buf.ReadBytes(0, (Int32)ms.Position).Crc();
            writer.Write(crc);

            // 写回去
            using (var ac = mmf.CreateAccessor(block))
            {
                ac.WriteArray(block.Position, buf, 0, buf.Length);
            }

            // 头部
            _head = mmf.CreateAccessor(bk1 + block.Position);

            // 哈希桶
            _buckets = mmf.CreateAccessor(bk2 + block.Position);

            // 链表数据
            _data = mmf.CreateAccessor(bk3 + block.Position);
        }

        /// <summary>获取区域</summary>
        /// <returns></returns>
        public Block GetArea() { return _block; }

        /// <summary>索引器最小大小</summary>
        public static Int32 MinSize { get { return 256 + 31 * 16 + 1024 * 1024; } }
        #endregion

        #region 核心方法
        public Block GetOrAdd(Byte[] key, Func<Byte[], Block> valueFactory)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (valueFactory == null) throw new ArgumentNullException(nameof(valueFactory));

            if (TryGetValue(key, out var result)) return result;

            TryAddInternal(key, valueFactory(key), false, true, out result);

            return result;
        }

        public Block AddOrUpdate(Byte[] key, Func<Byte[], Block> addValueFactory, Func<Byte[], Block, Block> updateValueFactory)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (addValueFactory == null) throw new ArgumentNullException(nameof(addValueFactory));
            if (updateValueFactory == null) throw new ArgumentNullException(nameof(updateValueFactory));

            Block val2;
            var result = default(Block);
            do
            {
                if (TryGetValue(key, out var val))
                {
                    val2 = updateValueFactory(key, val);
                    if (!TryUpdate(key, val2, val)) continue;

                    return val2;
                }
                val2 = addValueFactory(key);
            }
            while (!TryAddInternal(key, val2, false, true, out result));
            return result;
        }

        public Boolean ContainsKey(Byte[] key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return TryGetValue(key, out var val);
        }

        public Boolean TryGetValue(Byte[] key, out Block value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            for (var node = GetBucket(key); node != null; node = node.Next)
            {
                if (node.IsKey(key))
                {
                    value = node.Value;
                    return true;
                }
            }
            value = default(Block);
            return false;
        }

        public Boolean TryAdd(Byte[] key, Block value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return TryAddInternal(key, value, false, true, out var val);
        }

        private Boolean TryAddInternal(Byte[] key, Block value, Boolean updateIfExists, Boolean acquireLock, out Block resultingValue)
        {
            Bucket tables;
            IEqualityComparer<Byte[]> comparer;
            Boolean flag;
            //Boolean flag3;
            while (true)
            {
                tables = _table;
                comparer = tables.Comparer;
                var hashCode = comparer.GetHashCode(key);
                GetBucketAndLockNo(hashCode, out var num, out var num2, tables.Buckets.Length, tables.Locks.Length);
                flag = false;
                var flag2 = false;
                //flag3 = false;
                try
                {
                    if (acquireLock) Monitor.Enter(tables.Locks[num2], ref flag2);

                    if (tables == _table)
                    {
                        var num3 = 0;
                        DbNode node = null;
                        for (var node2 = tables.Buckets[num]; node2 != null; node2 = node2.Next)
                        {
                            if (comparer.Equals(node2.Key, key))
                            {
                                if (updateIfExists)
                                {
                                    var node3 = new DbNode(node2.Key, value, hashCode, node2.Next);
                                    if (node == null)
                                    {
                                        tables.Buckets[num] = node3;
                                    }
                                    else
                                    {
                                        node.Next = node3;
                                    }

                                    resultingValue = value;
                                }
                                else
                                {
                                    resultingValue = node2.Value;
                                }
                                return false;
                            }
                            node = node2;
                            num3++;
                        }
                        //if (num3 > 100 && HashHelpers.IsWellKnownEqualityComparer(comparer))
                        //{
                        //    flag = true;
                        //    flag3 = true;
                        //}
                        Volatile.Write(ref tables.Buckets[num], new DbNode(key, value, hashCode, tables.Buckets[num]));
                        checked
                        {
                            tables.CountPerLock[num2]++;
                            if (tables.CountPerLock[num2] > _budget)
                            {
                                flag = true;
                            }
                            break;
                        }
                    }
                }
                finally
                {
                    if (flag2)
                    {
                        Monitor.Exit(tables.Locks[num2]);
                    }
                }
            }
            if (flag)
            {
                //if (flag3)
                //{
                //    GrowTable(tables, (IEqualityComparer<Byte[]>)HashHelpers.GetRandomizedEqualityComparer(comparer), true, m_keyRehashCount);
                //}
                //else
                {
                    GrowTable(tables, tables.Comparer, false, _keyRehashCount);
                }
            }
            resultingValue = value;
            return true;
        }

        /// <summary>尝试移除并返回具有指定键的值</summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public Boolean TryRemove(Byte[] key, out Block value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            while (true)
            {
                var tables = _table;
                var comparer = tables.Comparer;
                GetBucketAndLockNo(comparer.GetHashCode(key), out var num, out var num2, tables.Buckets.Length, tables.Locks.Length);
                lock (tables.Locks[num2])
                {
                    if (tables == _table)
                    {
                        DbNode node = null;
                        for (var node2 = tables.Buckets[num]; node2 != null; node2 = node2.Next)
                        {
                            if (comparer.Equals(node2.Key, key))
                            {
                                if (node == null)
                                {
                                    Volatile.Write(ref tables.Buckets[num], node2.Next);
                                }
                                else
                                {
                                    node.Next = node2.Next;
                                }
                                value = node2.Value;
                                tables.CountPerLock[num2]--;
                                return true;
                            }
                            node = node2;
                        }
                        break;
                    }
                }
            }
            value = default(Block);
            return false;
        }

        public Boolean TryUpdate(Byte[] key, Block newValue, Block comparisonValue)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            IEqualityComparer<Block> @default = EqualityComparer<Block>.Default;
            while (true)
            {
                var tables = _table;
                var comparer = tables.Comparer;
                var hashCode = comparer.GetHashCode(key);
                GetBucketAndLockNo(hashCode, out var num, out var num2, tables.Buckets.Length, tables.Locks.Length);
                lock (tables.Locks[num2])
                {
                    if (tables == _table)
                    {
                        DbNode node = null;
                        for (var node2 = tables.Buckets[num]; node2 != null; node2 = node2.Next)
                        {
                            if (comparer.Equals(node2.Key, key))
                            {
                                if (@default.Equals(node2.Value, comparisonValue))
                                {
                                    var node3 = new DbNode(node2.Key, newValue, hashCode, node2.Next);
                                    if (node == null)
                                    {
                                        tables.Buckets[num] = node3;
                                    }
                                    else
                                    {
                                        node.Next = node3;
                                    }

                                    return true;
                                }
                                return false;
                            }
                            node = node2;
                        }
                        return false;
                    }
                }
            }
        }
        #endregion

        #region 辅助
        private void GrowTable(Bucket tables, IEqualityComparer<Byte[]> newComparer, Boolean regenerateHashKeys, Int32 rehashCount)
        {
            var locksAcquired = 0;
            try
            {
                AcquireLocks(0, 1, ref locksAcquired);
                if (regenerateHashKeys && rehashCount == _keyRehashCount)
                {
                    tables = _table;
                }
                else
                {
                    if (tables != _table)
                        return;
                    Int64 num = 0;
                    for (var index = 0; index < tables.CountPerLock.Length; ++index)
                        num += tables.CountPerLock[index];
                    if (num < tables.Buckets.Length / 4)
                    {
                        _budget = 2 * _budget;
                        if (_budget >= 0)
                            return;
                        _budget = Int32.MaxValue;
                        return;
                    }
                }
                var length1 = 0;
                var flag = false;
                try
                {
                    length1 = checked(tables.Buckets.Length * 2 + 1);
                    while (length1 % 3 == 0 || length1 % 5 == 0 || length1 % 7 == 0)
                        checked { length1 += 2; }
                    if (length1 > 2146435071)
                        flag = true;
                }
                catch (OverflowException)
                {
                    flag = true;
                }
                if (flag)
                {
                    length1 = 2146435071;
                    _budget = Int32.MaxValue;
                }
                AcquireLocks(1, tables.Locks.Length, ref locksAcquired);
                var locks = tables.Locks;
                if (tables.Locks.Length < 1024)
                {
                    locks = new Object[tables.Locks.Length * 2];
                    Array.Copy(tables.Locks, locks, tables.Locks.Length);
                    for (var length2 = tables.Locks.Length; length2 < locks.Length; ++length2)
                        locks[length2] = new Object();
                }
                var buckets = new DbNode[length1];
                var countPerLock = new Int32[locks.Length];
                DbNode next;
                for (var index = 0; index < tables.Buckets.Length; ++index)
                {
                    for (var node = tables.Buckets[index]; node != null; node = next)
                    {
                        next = node.Next;
                        var hashcode = node.HashCode;
                        if (regenerateHashKeys)
                            hashcode = newComparer.GetHashCode(node.Key);
                        GetBucketAndLockNo(hashcode, out var bucketNo, out var lockNo, buckets.Length, locks.Length);
                        buckets[bucketNo] = new DbNode(node.Key, node.Value, hashcode, buckets[bucketNo]);
                        checked { ++countPerLock[lockNo]; }
                    }
                }
                if (regenerateHashKeys)
                    _keyRehashCount = _keyRehashCount + 1;
                _budget = Math.Max(1, buckets.Length / locks.Length);
                _table = new Bucket(buckets, locks, countPerLock, newComparer);
            }
            finally
            {
                ReleaseLocks(0, locksAcquired);
            }
        }

        private DbNode GetBucket(Byte[] key)
        {
            var hash = key.Length == 0 ? 0 : BKDRHash(key);
            hash %= (UInt32)_table.Buckets.Length;

            return Volatile.Read(ref _table.Buckets[hash]);
        }

        private void GetBucketAndLockNo(Int32 hashcode, out Int32 bucketNo, out Int32 lockNo, Int32 bucketCount, Int32 lockCount)
        {
            bucketNo = (hashcode & 0x7FFFFFFF) % bucketCount;
            lockNo = bucketNo % lockCount;
        }

        private void AcquireAllLocks(ref Int32 locksAcquired)
        {
            AcquireLocks(0, 1, ref locksAcquired);
            AcquireLocks(1, _table.Locks.Length, ref locksAcquired);
        }

        private void AcquireLocks(Int32 fromInclusive, Int32 toExclusive, ref Int32 locksAcquired)
        {
            var locks = _table.Locks;
            for (var i = fromInclusive; i < toExclusive; i++)
            {
                var flag = false;
                try
                {
                    Monitor.Enter(locks[i], ref flag);
                }
                finally
                {
                    if (flag)
                    {
                        locksAcquired++;
                    }
                }
            }
        }

        private void ReleaseLocks(Int32 fromInclusive, Int32 toExclusive)
        {
            for (var index = fromInclusive; index < toExclusive; ++index)
                Monitor.Exit(_table.Locks[index]);
        }

        private static UInt32 BKDRHash(Byte[] data)
        {
            UInt32 hash = 0;
            for (var i = 0; i < data.Length; i++)
            {
                hash = hash * 131 + data[i];
            }

            return hash & 0x7FFF_FFFF;
        }

        private static UInt32 APHash(Byte[] data)
        {
            UInt32 hash = 0;
            for (var i = 0; i < data.Length; i++)
            {
                if ((i & 1) == 0)
                    hash ^= ((hash << 7) ^ data[i] ^ (hash >> 3));
                else
                    hash ^= (~((hash << 11) ^ data[i] ^ (hash >> 5)));
            }
            return hash & 0x7FFF_FFFF;
        }
        #endregion
    }
}