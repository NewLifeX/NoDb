using System;
using System.Collections.Concurrent;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace NewLife.NoDb.IO
{
    /// <summary>内存视图</summary>
    public class MemoryView : DisposeBase
    {
        #region 属性
        /// <summary>内存文件</summary>
        public MemoryFile File { get; }

        /// <summary>偏移。初始化后不再改变</summary>
        public Int64 Offset { get; }

        /// <summary>当前大小。根据需要自动扩容</summary>
        public Int64 Size { get; private set; }

        /// <summary>最大容量。初始化后不再改变</summary>
        public Int64 Capacity { get; }

        /// <summary>视图</summary>
        private MemoryMappedViewAccessor _view;

        /// <summary>版本</summary>
        private Int32 _Version;

        /// <summary>同步根对象</summary>
        public Object SyncRoot = new Object();
        #endregion

        #region 构造
        /// <summary>实例化一个内存视图</summary>
        /// <param name="file"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        public MemoryView(MemoryFile file, Int64 offset, Int64 size)
        {
            File = file ?? throw new ArgumentNullException(nameof(file));
            Offset = offset;
            Capacity = size;
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            _view.TryDispose();
            _view = null;
        }

        /// <summary>内存视图</summary>
        /// <returns></returns>
        public override String ToString() => $"[{File}]({Offset:n0}, {Size:n0}/{Capacity:n0})";
        #endregion

        #region 视图扩容
        /// <summary>获取视图，自动扩大</summary>
        /// <param name="offset">内存偏移</param>
        /// <param name="size">内存大小</param>
        /// <returns></returns>
        public MemoryMappedViewAccessor GetView(Int64 offset, Int64 size)
        {
            // 如果在已有范围内，则直接返回
            var maxsize = offset + size;
            if (_view != null && maxsize <= Size && _Version == File.Version) return _view;
            lock (SyncRoot)
            {
                if (_view != null && maxsize <= Size && _Version == File.Version) return _view;

                // 容量检查
                var remain = Capacity - Size;
                if (Capacity > 0 && remain < 0) throw new ArgumentOutOfRangeException(nameof(Size));

                // 最小增量 10%
                var step = maxsize - Size;
                if (step < 0)
                    step = 0;
                else if (step < Size / 10)
                    step = Size / 10;

                // 扩大视图，4k 对齐边界
                if (Capacity >= 4096)
                {
                    //maxsize += offset;
                    if (maxsize < 4096)
                        maxsize = 4096;
                    else
                    {
                        var n = maxsize % 4096;
                        if (n > 0) maxsize += 4096 - n;
                    }

                    // 底层边界4k对齐，Size不一定对齐
                    step = maxsize - Size;
                }

                // 注意末端边界，对齐后可能导致越界
                if (remain >= 0 && step > remain) step = remain;

                Size += step;

                // 容量检查
                if (Size < offset + size || Capacity > 0 && Size > Capacity) throw new ArgumentOutOfRangeException(nameof(Size));

                //// 销毁旧的
                //_view.TryDispose();
                var old = _view;

                // 映射文件扩容
                File.CheckCapacity(Offset + Size);

                _view = File.Map.CreateViewAccessor(Offset, Size);

                // 版本必须一致，如果内存文件扩容后版本改变，这里也要重新生成视图
                _Version = File.Version;

                // 销毁旧的
                old.TryDispose();

                return _view;
            }
        }

        /// <summary>获取视图数据流，自动扩大</summary>
        /// <param name="offset">内存偏移</param>
        /// <param name="size">内存大小</param>
        /// <returns></returns>
        public Stream GetStream(Int64 offset, Int64 size)
        {
            // 自动扩容
            var vw = GetView(offset, size);

            return File.Map.CreateViewStream(Offset, Size);
        }

        /// <summary>清除此视图的所有缓冲区并导致所有缓冲的数据写入到基础文件</summary>
        public void Flush() => _view?.Flush();
        #endregion

        #region 读写
        /// <summary>读取长整数</summary>
        /// <param name="position"></param>
        /// <returns></returns>
        public Int32 ReadInt32(Int64 position)
        {
            var view = GetView(position, 4);
            return view.ReadInt32(position);
        }

        /// <summary>写入长整数</summary>
        /// <param name="position"></param>
        /// <param name="value"></param>
        public void Write(Int64 position, Int32 value)
        {
            var view = GetView(position, 4);
            view.Write(position, value);
        }

        /// <summary>读取长整数</summary>
        /// <param name="position"></param>
        /// <returns></returns>
        public Int64 ReadInt64(Int64 position)
        {
            var view = GetView(position, 8);
            return view.ReadInt64(position);
        }

        /// <summary>写入长整数</summary>
        /// <param name="position"></param>
        /// <param name="value"></param>
        public void Write(Int64 position, Int64 value)
        {
            var view = GetView(position, 8);
            view.Write(position, value);
        }

        /// <summary>读取结构体</summary>
        /// <typeparam name="T">结构体类型</typeparam>
        /// <param name="position">位置</param>
        /// <param name="structure">结构体</param>
        public void Read<T>(Int64 position, out T structure) where T : struct
        {
            var view = GetView(position, SizeOf<T>());
            view.Read(position, out structure);
        }

        /// <summary>写入结构体</summary>
        /// <typeparam name="T">结构体类型</typeparam>
        /// <param name="position">位置</param>
        /// <param name="structure">结构体</param>
        /// <param name="size">结构体大小</param>
        public void Write<T>(Int64 position, ref T structure, Int32 size = 0) where T : struct
        {
            if (size <= 0) size = Marshal.SizeOf(structure);
            var view = GetView(position, size);
            view.Write(position, ref structure);
        }

        /// <summary>读取数组</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="position">位置</param>
        /// <param name="array">数组</param>
        /// <param name="offset">偏移</param>
        /// <param name="count">个数</param>
        public void ReadArray<T>(Int64 position, T[] array, Int32 offset, Int32 count) where T : struct
        {
            var view = GetView(position, SizeOf<T>() * count);
            view.ReadArray(position, array, offset, count);
        }

        /// <summary>写入数组</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="position">位置</param>
        /// <param name="array">数组</param>
        /// <param name="offset">偏移</param>
        /// <param name="count">个数</param>
        public void WriteArray<T>(Int64 position, T[] array, Int32 offset, Int32 count) where T : struct
        {
            var view = GetView(position, SizeOf<T>() * count);
            view.WriteArray(position, array, offset, count);
        }

        /// <summary>读取字节数组</summary>
        /// <param name="position"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public unsafe Byte[] ReadBytes(Int64 position, Int32 count)
        {
            var view = GetView(position, count);

            var ptr = (Byte*)0;
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            try
            {
                var p = new IntPtr(ptr);
                p = new IntPtr(p.ToInt64() + position);
                var arr = new Byte[count];
                Marshal.Copy(p, arr, 0, count);
                return arr;
            }
            finally
            {
                view.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }

        /// <summary>写入字节数组</summary>
        /// <param name="position"></param>
        /// <param name="data"></param>
        public unsafe void WriteBytes(Int64 position, Byte[] data)
        {
            var view = GetView(position, data.Length);

            var ptr = (Byte*)0;
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            try
            {
                var p = new IntPtr(ptr);
                p = new IntPtr(p.ToInt64() + position);
                Marshal.Copy(data, 0, p, data.Length);
            }
            finally
            {
                view.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }
        #endregion

        #region 辅助
        private static ConcurrentDictionary<Type, Int32> _sizeCache = new ConcurrentDictionary<Type, Int32>();
        private static Int32 SizeOf<T>() => _sizeCache.GetOrAdd(typeof(T), t => Marshal.SizeOf(t));
        #endregion
    }
}