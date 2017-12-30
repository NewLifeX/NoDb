using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存列表</summary>
    public class MemoryList<T> : MemoryCollection<T>, IList<T> where T : struct
    {
        #region 属性
        ///// <summary>访问器</summary>
        //public MemoryMappedViewAccessor View { get; }

        ///// <summary>容量</summary>
        //public Int32 Capacity { get; }

        ///// <summary>当前元素个数</summary>
        //public Int32 Count { get => View.ReadInt32(0); private set => View.Write(0, value); }

        ///// <summary>元素大小</summary>
        //private static Int32 _Size = Marshal.SizeOf(typeof(T));
        #endregion

        #region 构造
        /// <summary>实例化一个内存列表</summary>
        /// <param name="mmf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <param name="init">是否初始化为空</param>
        public MemoryList(MemoryMappedFile mmf, Int64 offset, Int64 size, Boolean init = true)
            : base(mmf, offset, size, init)
        {
        }
        #endregion

        #region 基本方法
        ///// <summary>索引器</summary>
        ///// <param name="index"></param>
        ///// <returns></returns>
        //public T this[Int32 index]
        //{
        //    get
        //    {
        //        if (index >= Count) throw new ArgumentOutOfRangeException(nameof(index));
        //        View.Read(index * _Size + 4, out T val);
        //        return val;
        //    }
        //    set
        //    {
        //        if (index >= Count) throw new ArgumentOutOfRangeException(nameof(index));

        //        View.Write(index * _Size + 4, ref value);
        //    }
        //}

        /// <summary>是否只读</summary>
        public Boolean IsReadOnly => false;

        ///// <summary>添加元素</summary>
        ///// <param name="item"></param>
        //public void Add(T item)
        //{
        //    var n = Count;
        //    if (n + 1 >= Capacity) throw new InvalidOperationException("容量不足");

        //    View.Write(n * _Size + 4, ref item);
        //    Count = n + 1;
        //}

        ///// <summary>批量插入</summary>
        ///// <param name="collection"></param>
        //public void AddRange(IEnumerable<T> collection)
        //{
        //    var arr = collection as T[] ?? collection.ToArray();
        //    if (arr.Length == 0) return;

        //    var n = Count;
        //    if (n + arr.Length >= Capacity) throw new InvalidOperationException("容量不足");

        //    View.WriteArray(n * _Size + 4, arr, 0, arr.Length);
        //    Count = n + arr.Length;
        //}

        ///// <summary>清空</summary>
        //public void Clear()
        //{
        //    Count = 0;
        //}

        /// <summary>是否包含</summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public Boolean Contains(T item) { return IndexOf(item) >= 0; }

        ///// <summary>拷贝</summary>
        ///// <param name="array"></param>
        ///// <param name="arrayIndex"></param>
        //public void CopyTo(T[] array, Int32 arrayIndex)
        //{
        //    //var n = Count;
        //    //for (Int32 i = 0, j = arrayIndex; i < n && j < array.Length; i++, j++)
        //    //{
        //    //    View.Read(i * _Size + 4, out T val);
        //    //    array[j] = val;
        //    //}

        //    var n = Count;
        //    if (n == 0) return;

        //    View.ReadArray(4, array, arrayIndex, n);
        //}

        /// <summary>查找</summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public Int32 IndexOf(T item)
        {
            var n = Count;
            for (var i = 0; i < n; i++)
            {
                View.Read(GetP(i), out T val);
                if (Equals(val, item)) return i;
            }

            return -1;
        }

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
                View.Read(GetP(i), out T val);
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

        ///// <summary>删除</summary>
        ///// <param name="index"></param>
        //public void RemoveAt(Int32 index)
        //{
        //    var n = Count;
        //    // index 之后前移一位
        //    for (var i = index + 1; i < n; i++)
        //    {
        //        View.Read(i * _Size + 4, out T val);
        //        View.Write((i - 1) * _Size + 4, ref val);
        //    }
        //    Count = n - 1;
        //}

        ///// <summary>枚举数</summary>
        ///// <returns></returns>
        //public IEnumerator<T> GetEnumerator()
        //{
        //    var n = Count;
        //    for (var i = 0; i < n; i++)
        //    {
        //        View.Read(i * _Size + 4, out T val);
        //        yield return val;
        //    }
        //}

        //IEnumerator IEnumerable.GetEnumerator() { return GetEnumerator(); }
        #endregion
    }
}