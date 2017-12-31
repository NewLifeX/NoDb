using System;
using System.Collections.Generic;
using NewLife.NoDb.IO;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存循环队列</summary>
    public class MemoryQueue<T> : MemoryCollection<T>, IReadOnlyCollection<T> where T : struct
    {
        #region 属性
        /// <summary>当前元素个数</summary>
        public Int64 Count { get => View.ReadInt64(0); private set => View.Write(0, value); }

        /// <summary>读取指针</summary>
        public Int64 ReadPosition { get => View.ReadInt64(8); private set => View.Write(8, value); }

        /// <summary>写入指针</summary>
        public Int64 WritePosition { get => View.ReadInt64(16); private set => View.Write(16, value); }

        /// <summary>获取集合大小</summary>
        /// <returns></returns>
        protected override Int64 GetCount() => Count;
        #endregion

        #region 构造
        static MemoryQueue() { _HeadSize = 24; }

        /// <summary>实例化一个内存队列</summary>
        /// <param name="mf">内存文件</param>
        /// <param name="offset">内存偏移</param>
        /// <param name="size">内存大小。为0时自动增长</param>
        /// <param name="init">是否初始化为空</param>
        public MemoryQueue(MemoryFile mf, Int64 offset = 0, Int64 size = 0, Boolean init = true) : base(mf, offset, size)
        {
            if (init) ReadPosition = WritePosition = 0;
        }
        #endregion

        #region 基本方法
        /// <summary>元素个数</summary>
        Int32 IReadOnlyCollection<T>.Count => (Int32)Count;

        /// <summary>获取栈顶</summary>
        /// <returns></returns>
        public T Peek()
        {
            var n = Count;
            if (n <= 0) throw new ArgumentOutOfRangeException(nameof(Count));

            var p = ReadPosition;
            View.Read<T>(GetP(p), out var val);

            return val;
        }

        /// <summary>弹出队列</summary>
        /// <returns></returns>
        public T Dequeue()
        {
            var n = Count;
            if (n <= 0) throw new ArgumentOutOfRangeException(nameof(Count));
            Count = n - 1;

            var p = ReadPosition;
            View.Read<T>(GetP(p), out var val);

            if (++p >= Capacity) p = 0;
            ReadPosition = p;

            return val;
        }

        /// <summary>进入队列</summary>
        /// <param name="item"></param>
        public void Enqueue(T item)
        {
            var n = Count;
            if (n + 1 >= Capacity) throw new InvalidOperationException("容量不足");
            Count = n + 1;

            var p = WritePosition;
            View.Write(GetP(p), ref item);

            if (++p >= Capacity) p = 0;
            WritePosition = p;
        }

        /// <summary>枚举数</summary>
        /// <returns></returns>
        public override IEnumerator<T> GetEnumerator()
        {
            var n = Count;
            var p = ReadPosition;
            for (var i = 0L; i < n; i++)
            {
                View.Read<T>(GetP(p), out var val);
                yield return val;

                if (++p >= Capacity) p = 0;
            }
        }
        #endregion
    }
}