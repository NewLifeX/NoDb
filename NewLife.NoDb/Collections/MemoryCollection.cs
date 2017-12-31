using System;
using System.Collections;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using NewLife.NoDb.IO;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存集合</summary>
    public abstract class MemoryCollection<T> : DisposeBase, IEnumerable, IEnumerable<T> where T : struct
    {
        #region 属性
        /// <summary>访问器</summary>
        public MemoryMappedViewAccessor View { get; }

        /// <summary>容量</summary>
        public Int64 Capacity { get; }
        #endregion

        #region 构造
        /// <summary>实例化一个内存列表</summary>
        /// <param name="mmf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        public MemoryCollection(MemoryFile mmf, Int64 offset, Int64 size)
        {
            if (offset == 0 && size == 0)
            {
                View = mmf.CreateView();
                size = View.Capacity;
            }
            else
                View = mmf.CreateView(offset, size);

            // 根据视图大小计算出可存储对象个数
            var n = size - _HeadSize;
            Capacity = n / _ItemSize;
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
        /// <summary>索引器</summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public T this[Int64 index]
        {
            get
            {
                if (index >= GetLength()) throw new ArgumentOutOfRangeException(nameof(index));
                View.Read<T>(GetP(index), out var val);
                return val;
            }
            set
            {
                if (index >= GetLength()) throw new ArgumentOutOfRangeException(nameof(index));

                View.Write(GetP(index), ref value);
            }
        }

        /// <summary>是否包含</summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public Boolean Contains(T item) { return IndexOf(item) >= 0; }

        /// <summary>查找</summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public Int64 IndexOf(T item)
        {
            var n = GetLength();
            for (var i = 0L; i < n; i++)
            {
                View.Read<T>(GetP(i), out var val);
                if (Equals(val, item)) return i;
            }

            return -1;
        }

        /// <summary>拷贝</summary>
        /// <param name="array"></param>
        /// <param name="arrayIndex"></param>
        public void CopyTo(T[] array, Int32 arrayIndex)
        {
            var n = GetLength();
            if (n == 0) return;

            if (n > array.Length) n = array.Length;

            View.ReadArray(GetP(0), array, arrayIndex, (Int32)n);
        }

        /// <summary>枚举数</summary>
        /// <returns></returns>
        public virtual IEnumerator<T> GetEnumerator()
        {
            var n = GetLength();
            for (var i = 0L; i < n; i++)
            {
                View.Read<T>(GetP(i), out var val);
                yield return val;
            }
        }

        IEnumerator IEnumerable.GetEnumerator() { return GetEnumerator(); }
        #endregion

        #region 辅助
        /// <summary>元素大小</summary>
        protected static Int32 _HeadSize = 0;

        /// <summary>元素大小</summary>
        private static Int32 _ItemSize = Marshal.SizeOf(typeof(T));

        /// <summary>获取位置</summary>
        /// <param name="idx"></param>
        /// <returns></returns>
        protected static Int64 GetP(Int64 idx) { return idx * _ItemSize + _HeadSize; }

        /// <summary>获取集合大小</summary>
        /// <returns></returns>
        protected abstract Int64 GetLength();
        #endregion
    }
}