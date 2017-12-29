using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存队列</summary>
    public class MemoryQueue<T> : MemoryCollection<T>, IReadOnlyCollection<T> where T : struct
    {
        #region 属性
        private Int64 _rw;
        /// <summary>读取指针</summary>
        public Int32 Read { get => View.ReadInt32(_rw); private set => View.Write(_rw, value); }

        /// <summary>写入指针</summary>
        public Int32 Write { get => View.ReadInt32(_rw + 4); private set => View.Write(_rw + 4, value); }
        #endregion

        #region 构造
        /// <summary>实例化一个内存队列</summary>
        /// <param name="mmf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <param name="init">是否初始化为空列表</param>
        public MemoryQueue(MemoryMappedFile mmf, Int64 offset, Int64 size, Boolean init = true)
            : base(mmf, offset, size - 4 - 4, init)
        {
        }
        #endregion

        #region 基本方法
        /// <summary>获取栈顶</summary>
        /// <returns></returns>
        public T Peek() { return this[Count - 1]; }

        /// <summary>弹出栈顶</summary>
        /// <returns></returns>
        public T Dequeue()
        {
            var idx = Count - 1;
            var val = this[idx];
            RemoveAt(idx);

            return val;
        }

        /// <summary>压栈</summary>
        /// <param name="item"></param>
        public void Enqueue(T item) { Add(item); }
        #endregion
    }
}