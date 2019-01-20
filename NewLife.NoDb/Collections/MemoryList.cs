using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NewLife.NoDb.IO;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存列表</summary>
    public class MemoryList<T> : MemoryCollection<T>, IList<T> where T : struct
    {
        #region 属性
        private Int32 _Count;
        /// <summary>当前元素个数</summary>
        public Int32 Count => _Count;

        /// <summary>获取集合大小</summary>
        /// <returns></returns>
        protected override Int32 GetCount() => Count;
        #endregion

        #region 构造
        static MemoryList() => _HeadSize = 8;

        /// <summary>实例化一个内存列表</summary>
        /// <param name="mf">内存文件</param>
        /// <param name="offset">内存偏移</param>
        /// <param name="size">内存大小。为0时自动增长</param>
        /// <param name="init">是否初始化为空</param>
        public MemoryList(MemoryFile mf, Int64 offset = 0, Int64 size = 0, Boolean init = true) : base(mf, offset, size)
        {
            if (init)
                OnSave();
            else
                OnLoad();
        }
        #endregion

        #region 基本方法
        /// <summary>元素个数</summary>
        Int32 ICollection<T>.Count => Count;

        /// <summary>是否只读</summary>
        Boolean ICollection<T>.IsReadOnly => false;

        T IList<T>.this[Int32 index] { get => this[index]; set => this[index] = value; }

        Int32 IList<T>.IndexOf(T item) => (Int32)IndexOf(item);

        void IList<T>.RemoveAt(Int32 index) => RemoveAt(index);

        /// <summary>添加元素</summary>
        /// <param name="item"></param>
        public void Add(T item)
        {
            var n = 0;
            do
            {
                n = Count;
                if (n >= Capacity) throw new InvalidOperationException("容量不足");
            }
            while (Interlocked.CompareExchange(ref _Count, n + 1, n) != n);

            View.Write(GetP(n), ref item);

            Commit();
        }

        /// <summary>批量插入</summary>
        /// <param name="collection"></param>
        public void AddRange(IEnumerable<T> collection)
        {
            var arr = collection as T[] ?? collection.ToArray();
            var size = arr.Length;
            if (size == 0) return;

            var n = 0;
            do
            {
                n = Count;
                if (n + size - 1 >= Capacity) throw new InvalidOperationException("容量不足");
            }
            while (Interlocked.CompareExchange(ref _Count, n + size, n) != n);

            View.WriteArray(GetP(n), arr, 0, arr.Length);

            Commit();
        }

        /// <summary>清空</summary>
        public void Clear()
        {
            _Count = 0;
            Commit();
        }

        /// <summary>插入</summary>
        /// <param name="index"></param>
        /// <param name="item"></param>
        public void Insert(Int32 index, T item)
        {
            var n = 0;
            do
            {
                n = Count;
                if (n >= Capacity) throw new InvalidOperationException("容量不足");
            }
            while (Interlocked.CompareExchange(ref _Count, n + 1, n) != n);

            // index 之后的元素后移一位
            for (var i = n - 1; i >= index; i--)
            {
                View.Read<T>(GetP(i), out var val);
                View.Write(GetP(i + 1), ref val);
            }

            View.Write(GetP(index), ref item);

            Commit();
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
            var n = Interlocked.Decrement(ref _Count);
            n++;

            // index 之后前移一位
            for (var i = index + 1; i < n; i++)
            {
                View.Read<T>(GetP(i), out var val);
                View.Write(GetP(i - 1), ref val);
            }
            Commit();
        }
        #endregion

        #region 定时保存
        /// <summary>定时保存数据到文件</summary>
        protected override void OnSave() => View.Write(0, _Count);

        private void OnLoad() => _Count = View.ReadInt32(0);
        #endregion
    }
}