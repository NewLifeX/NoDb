using System;
using System.Collections.Generic;
using NewLife.NoDb.IO;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存栈</summary>
    public class MemoryStack<T> : MemoryCollection<T>, IReadOnlyCollection<T> where T : struct
    {
        #region 属性
        /// <summary>当前元素个数</summary>
        public Int64 Count { get => View.ReadInt64(0); protected set => View.Write(0, value); }

        /// <summary>获取集合大小</summary>
        /// <returns></returns>
        protected override Int64 GetCount() => Count;
        #endregion

        #region 构造
        static MemoryStack() { _HeadSize = 8; }

        /// <summary>实例化一个内存栈</summary>
        /// <param name="mf">内存文件</param>
        /// <param name="offset">内存偏移</param>
        /// <param name="size">内存大小。为0时自动增长</param>
        /// <param name="init">是否初始化为空</param>
        public MemoryStack(MemoryFile mf, Int64 offset = 0, Int64 size = 0, Boolean init = true) : base(mf, offset, size)
        {
            if (init) Count = 0;
        }
        #endregion

        #region 基本方法
        /// <summary>元素个数</summary>
        Int32 IReadOnlyCollection<T>.Count => (Int32)Count;

        /// <summary>获取栈顶</summary>
        /// <returns></returns>
        public T Peek() { return this[Count - 1]; }

        /// <summary>弹出栈顶</summary>
        /// <returns></returns>
        public T Pop()
        {
            var idx = Count - 1;

            View.Read<T>(GetP(idx), out var val);

            Count = idx;

            return val;
        }

        /// <summary>压栈</summary>
        /// <param name="item"></param>
        public void Push(T item)
        {
            var n = Count;
            if (n + 1 >= Capacity) throw new InvalidOperationException("容量不足");

            View.Write(GetP(n), ref item);
            Count = n + 1;
        }
        #endregion
    }
}