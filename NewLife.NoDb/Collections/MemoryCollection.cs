using System;
using System.Collections;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存集合</summary>
    public abstract class MemoryCollection<T> : DisposeBase, IEnumerable, IEnumerable<T> where T : struct
    {
        #region 属性
        /// <summary>访问器</summary>
        public MemoryMappedViewAccessor View { get; }

        /// <summary>容量</summary>
        public Int32 Capacity { get; }
        #endregion

        #region 构造
        /// <summary>实例化一个内存列表</summary>
        /// <param name="mmf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        public MemoryCollection(MemoryMappedFile mmf, Int64 offset, Int64 size)
        {
            View = mmf.CreateViewAccessor(offset, size);

            // 根据视图大小计算出可存储对象个数
            var n = size - _HeadSize;
            Capacity = (Int32)(n / _ItemSize);
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
        public T this[Int32 index]
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
        public Int32 IndexOf(T item)
        {
            var n = GetLength();
            for (var i = 0; i < n; i++)
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

            View.ReadArray(GetP(0), array, arrayIndex, n);
        }

        /// <summary>枚举数</summary>
        /// <returns></returns>
        public virtual IEnumerator<T> GetEnumerator()
        {
            var n = GetLength();
            for (var i = 0; i < n; i++)
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
        protected static Int32 GetP(Int32 idx) { return idx * _ItemSize + _HeadSize; }

        /// <summary>获取集合大小</summary>
        /// <returns></returns>
        protected abstract Int32 GetLength();
        #endregion
    }
}