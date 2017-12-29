using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存栈</summary>
    public class MemoryStack<T> : MemoryCollection<T>, IReadOnlyCollection<T> where T : struct
    {
        #region 属性
        #endregion

        #region 构造
        /// <summary>实例化一个内存栈</summary>
        /// <param name="mmf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <param name="init">是否初始化为空列表</param>
        public MemoryStack(MemoryMappedFile mmf, Int64 offset, Int64 size, Boolean init = true)
            : base(mmf, offset, size, init)
        {
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
            var val = this[idx];
            RemoveAt(idx);

            return val;
        }

        /// <summary>压栈</summary>
        /// <param name="item"></param>
        public void Push(T item) { Add(item); }
        #endregion
    }
}