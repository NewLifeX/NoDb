using System;
using System.Collections;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存循环队列</summary>
    public class MemoryQueue<T> : DisposeBase, IReadOnlyCollection<T> where T : struct
    {
        #region 属性
        /// <summary>访问器</summary>
        public MemoryMappedViewAccessor View { get; }

        /// <summary>容量</summary>
        public Int32 Capacity { get; }

        /// <summary>当前元素个数</summary>
        public Int32 Count { get => View.ReadInt32(0); private set => View.Write(0, value); }

        /// <summary>读取指针</summary>
        public Int32 ReadPosition { get => View.ReadInt32(4); private set => View.Write(4, value); }

        /// <summary>写入指针</summary>
        public Int32 WritePosition { get => View.ReadInt32(8); private set => View.Write(8, value); }

        /// <summary>元素大小</summary>
        protected static Int32 _Size = Marshal.SizeOf(typeof(T));
        #endregion

        #region 构造
        /// <summary>实例化一个内存队列</summary>
        /// <param name="mmf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <param name="init">是否初始化为空</param>
        public MemoryQueue(MemoryMappedFile mmf, Int64 offset, Int64 size, Boolean init = true)
        {
            View = mmf.CreateViewAccessor(offset, size);

            // 根据视图大小计算出可存储对象个数
            var n = size - 8;
            Capacity = (Int32)(n / _Size);

            if (init) ReadPosition = WritePosition = 0;
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            View.TryDispose();
        }
        #endregion

        #region 基本方法
        /// <summary>获取栈顶</summary>
        /// <returns></returns>
        public T Peek()
        {
            var n = Count;
            if (n <= 0) throw new ArgumentOutOfRangeException(nameof(Count));

            var p = ReadPosition;
            View.Read(GetP(p), out T val);

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
            View.Read(GetP(p), out T val);

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
        public IEnumerator<T> GetEnumerator()
        {
            var n = Count;
            var p = ReadPosition;
            for (var i = 0; i < n; i++)
            {
                View.Read(GetP(p), out T val);
                yield return val;

                if (++p >= Capacity) p = 0;
            }
        }

        IEnumerator IEnumerable.GetEnumerator() { return GetEnumerator(); }
        #endregion

        #region 辅助
        private static Int32 GetP(Int32 idx) { return idx * _Size + 12; }
        #endregion
    }
}