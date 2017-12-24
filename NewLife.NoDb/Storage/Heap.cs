using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;

namespace NewLife.NoDb.Storage
{
    /// <summary>堆管理</summary>
    public class Heap
    {
        #region 属性
        private readonly Object SyncRoot = new Object();
        private AtomicHeader header;

        private readonly Space space = new Space();

        //updated every time after Serialize() invocation.
        private Int64 maxPositionPlusSize;

        /// <summary>使用中</summary>
        private readonly ConcurrentDictionary<Int64, Pointer> used = new ConcurrentDictionary<Int64, Pointer>();
        /// <summary>保留块</summary>
        private readonly ConcurrentDictionary<Int64, Pointer> reserved = new ConcurrentDictionary<Int64, Pointer>();

        private Int64 _Version;
        /// <summary>当前版本</summary>
        public Int64 Version { get { return _Version; } }

        private Int64 maxHandle;

        /// <summary>数据流</summary>
        public Stream Stream { get; private set; }

        /// <summary>分配策略</summary>
        public AllocationStrategy Strategy { get { return space.Strategy; } set { space.Strategy = value; } }

        /// <summary>数据大小</summary>
        public Int64 DataSize
        {
            get
            {
                //lock (SyncRoot)
                return used.Sum(kv => kv.Value.Block.Size);
            }
        }

        /// <summary>整体大小</summary>
        public Int64 Size { get { return Stream.Length; } }

        /// <summary>是否使用压缩</summary>
        public Boolean UseCompression { get { return header.UseCompression; } }
        #endregion

        #region 构造
        /// <summary>实例化数据堆</summary>
        /// <param name="stream"></param>
        /// <param name="useCompression"></param>
        /// <param name="strategy"></param>
        public Heap(Stream stream, Boolean useCompression = false, AllocationStrategy strategy = AllocationStrategy.FromTheCurrentBlock)
        {
            stream.Seek(0, SeekOrigin.Begin);

            Stream = stream;

            //space = new Space();

            //used = new Dictionary<Int64, Pointer>();
            //reserved = new Dictionary<Int64, Pointer>();

            // 建立新的头部
            if (stream.Length < AtomicHeader.SIZE)
            {
                header = new AtomicHeader
                {
                    UseCompression = useCompression
                };
                space.Add(new Block(AtomicHeader.SIZE, Int64.MaxValue - AtomicHeader.SIZE));
            }
            else // 读取已存在的头部
            {
                header = AtomicHeader.Deserialize(Stream);
                stream.Seek(header.SystemData.Position, SeekOrigin.Begin);
                Deserialize(new BinaryReader(stream));

                // 手工分配头部数据
                var ptr = space.Alloc(header.SystemData.Size);
                if (ptr.Position != header.SystemData.Position) throw new Exception("逻辑错误");
            }

            Strategy = strategy;

            _Version++;
        }

        /// <summary>实例化数据堆</summary>
        /// <param name="fileName"></param>
        /// <param name="useCompression"></param>
        /// <param name="strategy"></param>
        public Heap(String fileName, Boolean useCompression = false, AllocationStrategy strategy = AllocationStrategy.FromTheCurrentBlock)
            : this(new OptimizedFileStream(fileName, FileMode.OpenOrCreate), useCompression, strategy)
        {
        }
        #endregion

        private void FreeOldVersions()
        {
            var forRemove = new List<Int64>();

            foreach (var kv in reserved)
            {
                if (kv.Value.RefCount > 0) continue;

                space.Free(kv.Value.Block);
                forRemove.Add(kv.Key);
            }

            foreach (var handle in forRemove)
                reserved.Remove(handle);
        }

        private void InternalWrite(Int64 position, Int32 originalCount, Byte[] buffer, Int32 index, Int32 count)
        {
            var writer = new BinaryWriter(Stream);
            Stream.Seek(position, SeekOrigin.Begin);

            if (UseCompression)
                writer.Write(originalCount);

            writer.Write(buffer, index, count);
        }

        private Byte[] InternalRead(Int64 position, Int64 size)
        {
            var reader = new BinaryReader(Stream);
            Stream.Seek(position, SeekOrigin.Begin);

            Byte[] buffer;

            if (!UseCompression)
                buffer = reader.ReadBytes((Int32)size);
            else
            {
                var raw = new Byte[reader.ReadInt32()];
                buffer = reader.ReadBytes((Int32)size - sizeof(Int32));

                using (var stream = new MemoryStream(buffer))
                {
                    using (var decompress = new DeflateStream(stream, CompressionMode.Decompress))
                        decompress.Read(raw, 0, raw.Length);
                }

                buffer = raw;
            }

            return buffer;
        }

        private void Serialize(BinaryWriter writer)
        {
            maxPositionPlusSize = AtomicHeader.SIZE;

            writer.Write(maxHandle);
            writer.Write(_Version);

            // 写入空闲空间设置
            space.Serialize(writer);

            // 写入已使用
            writer.Write(used.Count);
            foreach (var kv in used)
            {
                writer.Write(kv.Key);
                kv.Value.Serialize(writer);

                var posPlusSize = kv.Value.Block.PositionPlusSize;
                if (posPlusSize > maxPositionPlusSize)
                    maxPositionPlusSize = posPlusSize;
            }

            // 写入保留
            writer.Write(reserved.Count);
            foreach (var kv in reserved)
            {
                writer.Write(kv.Key);
                kv.Value.Serialize(writer);

                var posPlusSize = kv.Value.Block.PositionPlusSize;
                if (posPlusSize > maxPositionPlusSize)
                    maxPositionPlusSize = posPlusSize;
            }
        }

        private void Deserialize(BinaryReader reader)
        {
            maxHandle = reader.ReadInt64();
            _Version = reader.ReadInt64();

            // 读取空闲空间设置
            space.Deserealize(reader);

            // 读取已使用
            var count = reader.ReadInt32();
            for (var i = 0; i < count; i++)
            {
                var handle = reader.ReadInt64();
                var pointer = Pointer.Deserialize(reader);
                used.TryAdd(handle, pointer);
            }

            // 读取保留
            count = reader.ReadInt32();
            for (var i = 0; i < count; i++)
            {
                var handle = reader.ReadInt64();
                var pointer = Pointer.Deserialize(reader);
                reserved.TryAdd(handle, pointer);
            }
        }

        //public Byte[] Tag
        //{
        //    get
        //    {
        //        lock (SyncRoot)
        //            return header.Tag;
        //    }
        //    set
        //    {
        //        lock (SyncRoot)
        //            header.Tag = value;
        //    }
        //}

        /// <summary>获得新的句柄</summary>
        /// <returns></returns>
        public Int64 ObtainNewHandle()
        {
            //lock (SyncRoot)
            //    return maxHandle++;
            var rs = maxHandle;
            Interlocked.Increment(ref maxHandle);
            return rs;
        }

        /// <summary>释放句柄</summary>
        /// <param name="handle"></param>
        public void Release(Int64 handle)
        {
            //lock (SyncRoot)
            //{
            if (!used.TryGetValue(handle, out var pointer)) return;

            if (pointer.Version == _Version)
                space.Free(pointer.Block);
            else
            {
                pointer.IsReserved = true;
                reserved.TryAdd(handle, pointer);
            }

            used.Remove(handle);
            //}
        }

        /// <summary>句柄是否已使用</summary>
        /// <param name="handle"></param>
        /// <returns></returns>
        public Boolean Exists(Int64 handle)
        {
            //lock (SyncRoot)
            return used.ContainsKey(handle);
        }

        /// <summary>
        /// Before writting, handle must be obtained (registered).
        /// New block will be written always with version = CurrentVersion
        /// If new block is written to handle and the last block of this handle have same version with the new one, occupied space by the last block will be freed.
        /// </summary>
        public void Write(Int64 handle, Byte[] buffer, Int32 index, Int32 count)
        {
            var originalCount = count;

            if (UseCompression)
            {
                using (var stream = new MemoryStream())
                {
                    using (var compress = new DeflateStream(stream, CompressionMode.Compress, true))
                        compress.Write(buffer, index, count);

                    buffer = stream.GetBuffer();
                    index = 0;
                    count = (Int32)stream.Length;
                }
            }

            lock (SyncRoot)
            {
                if (handle >= maxHandle) throw new ArgumentException("无效句柄");

                if (used.TryGetValue(handle, out var pointer))
                {
                    if (pointer.Version == _Version)
                        space.Free(pointer.Block);
                    else
                    {
                        pointer.IsReserved = true;
                        reserved.TryAdd(handle, pointer);
                    }
                }

                Int64 size = UseCompression ? sizeof(Int32) + count : count;
                var ptr = space.Alloc(size);
                used[handle] = pointer = new Pointer(_Version, ptr);

                InternalWrite(ptr.Position, originalCount, buffer, index, count);
            }
        }

        /// <summary>读取句柄处数据</summary>
        /// <param name="handle"></param>
        /// <returns></returns>
        public Byte[] Read(Int64 handle)
        {
            lock (SyncRoot)
            {
                if (!used.TryGetValue(handle, out var pointer)) throw new ArgumentException("该句柄目标没有数据存在");

                var ptr = pointer.Block;
                Debug.Assert(ptr != Block.NULL);

                return InternalRead(ptr.Position, ptr.Size);
            }
        }

        /// <summary>提交</summary>
        public void Commit()
        {
            //lock (SyncRoot)
            //{
            Stream.Flush();

            FreeOldVersions();

            using (var ms = new MemoryStream())
            {
                if (header.SystemData != Block.NULL)
                    space.Free(header.SystemData);

                Serialize(new BinaryWriter(ms));

                var ptr = space.Alloc(ms.Length);
                Stream.Seek(ptr.Position, SeekOrigin.Begin);
                Stream.Write(ms.GetBuffer(), 0, (Int32)ms.Length);

                header.SystemData = ptr;

                // 写文件头
                header.Serialize(Stream);

                if (ptr.PositionPlusSize > maxPositionPlusSize)
                    maxPositionPlusSize = ptr.PositionPlusSize;
            }

            Stream.Flush();

            // 尝试清除多余空间
            if (Stream.Length > maxPositionPlusSize)
                Stream.SetLength(maxPositionPlusSize);

            _Version++;
            //}
        }

        /// <summary>关闭</summary>
        public void Close() { Stream.Close(); }

        /// <summary>获取最后</summary>
        /// <param name="atVersion"></param>
        /// <returns></returns>
        public IEnumerable<KeyValuePair<Int64, Byte[]>> GetLatest(Int64 atVersion)
        {
            var list = new List<KeyValuePair<Int64, Pointer>>();

            //lock (SyncRoot)
            //{
            foreach (var kv in used.Union(reserved))
            {
                var handle = kv.Key;
                var pointer = kv.Value;

                if (pointer.Version >= atVersion && pointer.Version < _Version)
                {
                    list.Add(new KeyValuePair<Int64, Pointer>(handle, pointer));
                    pointer.RefCount++;
                }
            }
            //}

            foreach (var kv in list)
            {
                var handle = kv.Key;
                var pointer = kv.Value;

                Byte[] buffer;
                //lock (SyncRoot)
                //{
                buffer = InternalRead(pointer.Block.Position, pointer.Block.Size);
                pointer.RefCount--;
                if (pointer.IsReserved && pointer.RefCount <= 0)
                {
                    space.Free(pointer.Block);
                    reserved.Remove(handle);
                }
                //}

                yield return new KeyValuePair<Int64, Byte[]>(handle, buffer);
            }
        }

        /// <summary>获取用户空间</summary>
        /// <returns></returns>
        public KeyValuePair<Int64, Block>[] GetUsedSpace()
        {
            //lock (SyncRoot)
            //{
            var array = new KeyValuePair<Int64, Block>[used.Count + reserved.Count];

            var idx = 0;
            foreach (var kv in used.Union(reserved))
                array[idx++] = new KeyValuePair<Int64, Block>(kv.Value.Version, kv.Value.Block);

            return array;
            //}
        }
    }
}