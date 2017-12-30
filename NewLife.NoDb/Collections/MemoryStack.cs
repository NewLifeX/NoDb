using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存栈</summary>
    public class MemoryStack<T> : MemoryCollection<T>, IReadOnlyCollection<T> where T : struct
    {
        #region 属性
        /// <summary>当前元素个数</summary>
        public Int32 Count { get => View.ReadInt32(0); protected set => View.Write(0, value); }

        /// <summary>获取集合大小</summary>
        /// <returns></returns>
        protected override Int32 GetLength() => Count;
        #endregion

        #region 构造
        static MemoryStack() { _HeadSize = 4; }

        /// <summary>实例化一个内存栈</summary>
        /// <param name="mmf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <param name="init">是否初始化为空</param>
        public MemoryStack(MemoryMappedFile mmf, Int64 offset, Int64 size, Boolean init = true) : base(mmf, offset, size)
        {
            if (init) Count = 0;
        }
        #endregion

        #region 基本方法
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