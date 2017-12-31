using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Linq;
using NewLife.NoDb.IO;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存列表</summary>
    public class MemoryList<T> : MemoryCollection<T>, IList<T> where T : struct
    {
        #region 属性
        /// <summary>当前元素个数</summary>
        public Int64 Count { get => View.ReadInt64(0); protected set => View.Write(0, value); }

        /// <summary>获取集合大小</summary>
        /// <returns></returns>
        protected override Int64 GetLength() => Count;
        #endregion

        #region 构造
        static MemoryList() { _HeadSize = 8; }

        /// <summary>实例化一个内存列表</summary>
        /// <param name="mmf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <param name="init">是否初始化为空</param>
        public MemoryList(MemoryFile mmf, Int64 offset, Int64 size, Boolean init = true)
            : base(mmf, offset, size)
        {
            if (init) Count = 0;
        }
        #endregion

        #region 基本方法
        /// <summary>元素个数</summary>
        Int32 ICollection<T>.Count => (Int32)Count;

        /// <summary>是否只读</summary>
        Boolean ICollection<T>.IsReadOnly => false;

        T IList<T>.this[Int32 index] { get => this[index]; set => this[index] = value; }

        Int32 IList<T>.IndexOf(T item) { return (Int32)IndexOf(item); }

        void IList<T>.RemoveAt(Int32 index) { RemoveAt(index); }

        /// <summary>添加元素</summary>
        /// <param name="item"></param>
        public void Add(T item)
        {
            var n = Count;
            if (n + 1 >= Capacity) throw new InvalidOperationException("容量不足");

            View.Write(GetP(n), ref item);
            Count = n + 1;
        }

        /// <summary>批量插入</summary>
        /// <param name="collection"></param>
        public void AddRange(IEnumerable<T> collection)
        {
            var arr = collection as T[] ?? collection.ToArray();
            if (arr.Length == 0) return;

            var n = Count;
            if (n + arr.Length >= Capacity) throw new InvalidOperationException("容量不足");

            View.WriteArray(GetP(n), arr, 0, arr.Length);
            Count = n + arr.Length;
        }

        /// <summary>清空</summary>
        public void Clear() { Count = 0; }

        /// <summary>插入</summary>
        /// <param name="index"></param>
        /// <param name="item"></param>
        public void Insert(Int32 index, T item)
        {
            var n = Count;
            if (n + 1 >= Capacity) throw new InvalidOperationException("容量不足");

            // index 之后的元素后移一位
            for (var i = n - 1; i >= index; i--)
            {
                View.Read<T>(GetP(i), out var val);
                View.Write(GetP(i + 1), ref val);
            }

            View.Write(GetP(index), ref item);
            Count = n + 1;
        }

        /// <summary>删除</summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public Boolean Remove(T item)
        {
            var idx = IndexOf(item);
            if (idx < 0) return false;

            RemoveAt(idx);

            return true;
        }

        /// <summary>删除</summary>
        /// <param name="index"></param>
        public void RemoveAt(Int64 index)
        {
            var n = Count;
            // index 之后前移一位
            for (var i = index + 1; i < n; i++)
            {
                View.Read<T>(GetP(i), out var val);
                View.Write(GetP(i - 1), ref val);
            }
            Count = n - 1;
        }
        #endregion
    }
}