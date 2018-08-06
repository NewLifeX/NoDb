using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using NewLife.NoDb.IO;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存数组</summary>
    public class MemoryArray<T> : MemoryCollection<T>, IList<T> where T : struct
    {
        #region 属性
        /// <summary>长度</summary>
        public Int32 Length { get; }

        /// <summary>获取集合大小</summary>
        /// <returns></returns>
        protected override Int32 GetCount() => Length;
        #endregion

        #region 构造
        static MemoryArray() { _HeadSize = 0; }

        /// <summary>实例化一个内存数组</summary>
        /// <param name="mf">内存文件</param>
        /// <param name="length">数组长度</param>
        /// <param name="offset">内存偏移</param>
        public MemoryArray(MemoryFile mf, Int64 length, Int64 offset = 0) : base(mf, offset, length * Marshal.SizeOf(typeof(T)))
        {
            Length = Capacity;
        }
        #endregion

        #region 基本方法
        /// <summary>清空数组</summary>
        public void Clear()
        {
            var arr = new T[Length];
            View.WriteArray(0, arr, 0, arr.Length);
        }

        T IList<T>.this[Int32 index] { get => this[index]; set => this[index] = value; }

        Int32 IList<T>.IndexOf(T item) { return (Int32)IndexOf(item); }
        #endregion

        #region IList<T>接口
        Int32 ICollection<T>.Count => (Int32)Length;

        Boolean ICollection<T>.IsReadOnly => true;

        void IList<T>.Insert(Int32 index, T item) => throw new NotImplementedException();

        void IList<T>.RemoveAt(Int32 index) => throw new NotImplementedException();

        void ICollection<T>.Add(T item) => throw new NotImplementedException();

        Boolean ICollection<T>.Remove(T item) => throw new NotImplementedException();
        #endregion
    }
}