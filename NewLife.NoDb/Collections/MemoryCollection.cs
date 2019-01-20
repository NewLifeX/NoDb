using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using NewLife.NoDb.IO;
using NewLife.Threading;

namespace NewLife.NoDb.Collections
{
    /// <summary>内存集合</summary>
    public abstract class MemoryCollection<T> : DisposeBase, IEnumerable, IEnumerable<T> where T : struct
    {
        #region 属性
        /// <summary>访问器</summary>
        public MemoryView View { get; }

        /// <summary>容量</summary>
        public Int32 Capacity { get; }
        #endregion

        #region 构造
        /// <summary>实例化一个内存集合</summary>
        /// <param name="mf">内存文件</param>
        /// <param name="offset">内存偏移</param>
        /// <param name="size">内存大小。为0时自动增长</param>
        public MemoryCollection(MemoryFile mf, Int64 offset, Int64 size)
        {
            if (offset == 0 && size == 0)
            {
                View = mf.CreateView();
                size = View.Capacity;
            }
            else
                View = mf.CreateView(offset, size);

            // 根据视图大小计算出可存储对象个数
            var n = size - _HeadSize;
            Capacity = (Int32)(n / _ItemSize);
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            _Timer.TryDispose();
            _Timer = null;

            // 关闭前处理未保存的数据
            DoSave(null);

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
                if (index >= GetCount()) throw new ArgumentOutOfRangeException(nameof(index));
                View.Read<T>(GetP(index), out var val);
                return val;
            }
            set
            {
                if (index >= GetCount()) throw new ArgumentOutOfRangeException(nameof(index));

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
            var n = GetCount();
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
            var n = GetCount();
            if (n == 0) return;

            if (n > array.Length) n = array.Length;

            View.ReadArray(GetP(0), array, arrayIndex, n);
        }

        /// <summary>枚举数</summary>
        /// <returns></returns>
        public virtual IEnumerator<T> GetEnumerator()
        {
            var n = GetCount();
            for (var i = 0L; i < n; i++)
            {
                View.Read<T>(GetP(i), out var val);
                yield return val;
            }
        }

        IEnumerator IEnumerable.GetEnumerator() { return GetEnumerator(); }
        #endregion

        #region 定时保存
        /// <summary>定时保存数据的周期。默认1000ms</summary>
        public Int32 Period { get; set; } = 1000;

        /// <summary>修改完以后需要提交</summary>
        public virtual void Commit()
        {
            Interlocked.Increment(ref _version);

            StartTimer();
        }

        /// <summary>开启定时器</summary>
        private void StartTimer()
        {
            if (_Timer != null) return;
            lock (this)
            {
                if (_Timer != null) return;

                var p = Period;
                if (p < 100) p = 100;
                _Timer = new TimerX(DoSave, null, p, p, "NoDb") { Async = true };
            }
        }

        private TimerX _Timer;
        private Int32 _version;
        private void DoSave(Object state)
        {
            var v = _version;
            if (v > 0)
            {
                lock (View.SyncRoot)
                {
                    OnSave();

                    Flush();
                }

                Interlocked.Add(ref _version, -v);
            }

            var p = Period;
            if (p < 100) p = 100;

            var tmr = _Timer;
            if (tmr != null && p != tmr.Period)
            {
                tmr.Period = p;
                tmr.SetNext(p);
            }
        }

        /// <summary>定时保存数据到文件</summary>
        protected virtual void OnSave() { }

        /// <summary>清除此视图的所有缓冲区并导致所有缓冲的数据写入到基础文件</summary>
        public void Flush() => View.Flush();
        #endregion

        #region 辅助
        /// <summary>元素大小</summary>
        protected static Int32 _HeadSize = 0;

        /// <summary>元素大小</summary>
        protected static Int32 _ItemSize = Marshal.SizeOf(typeof(T));

        /// <summary>获取位置</summary>
        /// <param name="idx"></param>
        /// <returns></returns>
        protected static Int64 GetP(Int64 idx) => (idx * _ItemSize) + _HeadSize;

        /// <summary>获取集合大小</summary>
        /// <returns></returns>
        protected abstract Int32 GetCount();
        #endregion
    }
}