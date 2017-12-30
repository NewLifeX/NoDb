using System;
using System.Collections;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存数组</summary>
    public class MemoryArray<T> : DisposeBase, IList<T> where T : struct
    {
        #region 属性
        /// <summary>访问器</summary>
        public MemoryMappedViewAccessor View { get; }

        /// <summary>长度</summary>
        public Int32 Length { get; }

        /// <summary>元素大小</summary>
        protected static Int32 _Size = Marshal.SizeOf(typeof(T));
        #endregion

        #region 构造
        /// <summary>实例化一个内存数组</summary>
        /// <param name="mmf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <param name="init">是否初始化为空</param>
        public MemoryArray(MemoryMappedFile mmf, Int64 offset, Int64 size, Boolean init = true)
        {
            View = mmf.CreateViewAccessor(offset, size);

            // 根据视图大小计算出可存储对象个数
            var n = size;
            Length = (Int32)(n / _Size);

            if (init) Clear();
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
                if (index >= Length) throw new ArgumentOutOfRangeException(nameof(index));
                View.Read(GetP(index), out T val);
                return val;
            }
            set
            {
                if (index >= Length) throw new ArgumentOutOfRangeException(nameof(index));

                View.Write(GetP(index), ref value);
            }
        }

        /// <summary>清空数组</summary>
        public void Clear()
        {
            var arr = new T[Length];
            View.WriteArray(0, arr, 0, arr.Length);
        }

        /// <summary>查找</summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public Int32 IndexOf(T item)
        {
            var n = Length;
            for (var i = 0; i < n; i++)
            {
                View.Read(i * _Size + 4, out T val);
                if (Equals(val, item)) return i;
            }

            return -1;
        }

        /// <summary>是否包含</summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public Boolean Contains(T item) { return IndexOf(item) >= 0; }

        /// <summary>拷贝</summary>
        /// <param name="array"></param>
        /// <param name="arrayIndex"></param>
        public void CopyTo(T[] array, Int32 arrayIndex)
        {
            var n = Length;
            if (n == 0) return;

            View.ReadArray(GetP(0), array, arrayIndex, n);
        }

        /// <summary>枚举数</summary>
        /// <returns></returns>
        public IEnumerator<T> GetEnumerator()
        {
            var n = Length;
            for (var i = 0; i < n; i++)
            {
                View.Read(GetP(i), out T val);
                yield return val;
            }
        }

        IEnumerator IEnumerable.GetEnumerator() { return GetEnumerator(); }
        #endregion

        #region IList<T>接口
        Int32 ICollection<T>.Count => Length;

        Boolean ICollection<T>.IsReadOnly => true;

        void IList<T>.Insert(Int32 index, T item) => throw new NotImplementedException();

        void IList<T>.RemoveAt(Int32 index) => throw new NotImplementedException();

        void ICollection<T>.Add(T item) => throw new NotImplementedException();

        Boolean ICollection<T>.Remove(T item) => throw new NotImplementedException();
        #endregion

        #region 辅助
        private static Int32 GetP(Int32 idx) { return idx * _Size; }
        #endregion
    }
}