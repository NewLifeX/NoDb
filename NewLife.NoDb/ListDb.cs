using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using NewLife.Log;
using NewLife.NoDb.IO;
using NewLife.NoDb.Storage;
using NewLife.Threading;

namespace NewLife.NoDb
{
    /// <summary>列表数据库</summary>
    /// <remarks>
    /// 以顺序整数为键，例如，以秒数为键按天分库，存储时序数据。
    /// 常用操作 Get/Set/Add
    /// </remarks>
    public class ListDb : DisposeBase
    {
        #region 属性
        /// <summary>幻数</summary>
        public const String Magic = "ListDb";
        private const Int32 HEADER_SIZE = 1024;
        private const Int64 MAX_SIZE = 2L * 1024 * 1024 * 1024;
        private const Int32 BLOCK_SIZE = sizeof(Int64) + sizeof(Int64);

        /// <summary>映射文件</summary>
        public MemoryFile File { get; }

        /// <summary>是否只读</summary>
        public Boolean ReadOnly { get; }

        /// <summary>版本</summary>
        public Byte Version { get; private set; } = 1;

        /// <summary>数据槽，记录每个数据块的位置</summary>
        /// <remarks>为了性能，初始化时直接把数据槽载入托管内存</remarks>
        public IList<Block> Slots { get; private set; }

        /// <summary>元素个数</summary>
        public Int32 Count => Slots == null ? 0 : Slots.Count;

        /// <summary>数据区</summary>
        private Heap Heap { get; set; }

        /// <summary>访问器</summary>
        private MemoryView View { get; }

        private Block _SlotData;
        #endregion

        #region 构造
        /// <summary>实例化数据库</summary>
        /// <param name="file">文件</param>
        /// <param name="readOnly">只读</param>
        /// <param name="init">自动初始化</param>
        public ListDb(String file, Boolean readOnly, Boolean init = true)
        {
            File = new MemoryFile(file, readOnly);
            ReadOnly = readOnly;

            if (readOnly)
            {
                View = File.CreateView(HEADER_SIZE, MAX_SIZE);
            }
            else
            {
                Heap = new Heap(File, HEADER_SIZE, MAX_SIZE, false);
                View = Heap.View;
            }

            // 加大步进增长率，减少映射次数，提升性能
            //View.StepRate = 100;

            // 加载
            if (init) Init();
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void Dispose(Boolean disposing)
        {
            base.Dispose(disposing);

            _timer.TryDispose();

            if (_commits > 0) Commit();

            Heap.TryDispose();
            Slots.TryDispose();
            View.TryDispose();
            File.TryDispose();
        }

        /// <summary>堆管理</summary>
        /// <returns></returns>
        public override String ToString()
        {
            var vw = View;

            return $"[{vw.File}]({vw.Offset:X8}, {vw.Capacity:X8}) Count={Count:n0}";
        }
        #endregion

        #region 读写
        /// <summary>初始化。初始化之后才能使用</summary>
        public void Init()
        {
            var hp = Heap;
            if (hp != null)
            {
                Heap.Log = Log;
                Heap.Init();
            }

            // 读取配置
            Read();

            WriteLog("列表库 {0}", this);
        }

        private Boolean Read()
        {
            using (var vw = File.CreateView(0, HEADER_SIZE))
            {
                var buf = vw.ReadBytes(0, 64);
                var reader = new BinaryReader(new MemoryStream(buf));

                // 幻数
                var magic = reader.ReadBytes(Magic.Length).ToStr();
                if (Magic != magic) return false;

                // 版本
                Version = reader.ReadByte();

                // 标记
                var flag = reader.ReadByte();

                // 索引器位置和个数
                var blk = new Block
                {
                    Position = reader.ReadInt64(),
                    Size = reader.ReadInt64(),
                };
                _SlotData = blk;

                // 加载数据槽进入托管内存，以加快查找速度
                var ss = new List<Block>();
                buf = View.ReadBytes(blk.Position, (Int32)blk.Size);
                reader = new BinaryReader(new MemoryStream(buf));
                var n = blk.Size / BLOCK_SIZE;
                for (var i = 0; i < n; i++)
                {
                    ss.Add(new Block
                    {
                        Position = reader.ReadInt64(),
                        Size = reader.ReadInt64(),
                    });
                }

                Slots = ss;
            }

            return true;
        }

        private void Write()
        {
            using (var vw = File.CreateView(0, HEADER_SIZE))
            {
                var ms = new MemoryStream();
                var writer = new BinaryWriter(ms);

                // 幻数
                writer.Write(Magic.GetBytes());

                // 版本
                writer.Write(Version);

                // 标记
                Byte flag = 0;
                writer.Write(flag);

                // 如果有数据，则需要写入
                var ss = Slots;
                var n = ss == null ? 0 : ss.Count;
                var len = n * BLOCK_SIZE;

                // 大小不同时，需要重新分配
                var blk = _SlotData;
                if (blk.Size != len)
                {
                    if (blk.Size > 0) Heap.Free(blk);
                    blk = _SlotData = Heap.Alloc(len);
                }

                writer.Write(blk.Position);
                writer.Write(blk.Size);

                vw.WriteBytes(0, ms.ToArray());

                // 写入数据槽
                if (n > 0)
                {
                    ms = new MemoryStream();
                    writer = new BinaryWriter(ms);
                    for (var i = 0; i < n; i++)
                    {
                        writer.Write(ss[i].Position);
                        writer.Write(ss[i].Size);
                    }
                    View.WriteBytes(blk.Position, ms.ToArray());
                }
            }
        }

        private Int32 _commits;
        /// <summary>提交修改</summary>
        public void Commit()
        {
            var mts = _commits;
            if (mts == 0) return;

            try
            {
                Write();

                Interlocked.Add(ref _commits, -mts);
            }
            catch (ObjectDisposedException) { }
        }

        private TimerX _timer;
        private void SetChange()
        {
            Interlocked.Increment(ref _commits);

            if (_timer == null)
            {
                lock (this)
                {
                    _timer ??= new TimerX(s => Commit(), null, 0, 1_000, "NoDb") { Async = true, };
                }
            }
        }
        #endregion

        #region 基础方法
        /// <summary>获取 数据</summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public Byte[] Get(Int32 index)
        {
            var ss = Slots;
            if (ss == null || index < 0 || index >= ss.Count) throw new ArgumentOutOfRangeException(nameof(index));

            var bk = ss[index];
            if (bk.Position == 0 || bk.Size == 0) return null;

            return View.ReadBytes(bk.Position, (Int32)bk.Size);
        }

        /// <summary>设置 数据</summary>
        /// <param name="index"></param>
        /// <param name="value"></param>
        public void Set(Int32 index, Byte[] value)
        {
            var ss = Slots;
            if (ss == null || index < 0 || index >= ss.Count) throw new ArgumentOutOfRangeException(nameof(index));

            var bk = ss[index];
            if (bk.Position == 0 || bk.Size == 0) bk = Heap.Alloc(value.Length);

            // View内部竟然没有叠加偏移量
            View.WriteBytes(bk.Position, value);

            ss[index] = bk;
        }

        /// <summary>向列表默认添加项</summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public Int32 Add(Byte[] value)
        {
            var ss = Slots;
            if (ss == null) ss = Slots = new List<Block>(1024);

            var n = ss.Count;
            var bk = Heap.Alloc(value.Length);
            ss.Add(bk);

            View.WriteBytes(bk.Position, value);

            SetChange();

            return n;
        }

        /// <summary>指定位置插入</summary>
        /// <param name="index"></param>
        /// <param name="item"></param>
        public void Insert(Int32 index, Byte[] item)
        {
            var ss = Slots;
            if (ss == null || index < 0 || index >= ss.Count) throw new ArgumentOutOfRangeException(nameof(index));

            var bk = Heap.Alloc(item.Length);
            ss.Insert(index, bk);

            View.WriteBytes(bk.Position, item);

            SetChange();
        }

        /// <summary>删除指定位置</summary>
        /// <param name="index"></param>
        public void RemoveAt(Int32 index)
        {
            var ss = Slots;
            if (ss == null || index < 0 || index >= ss.Count) throw new ArgumentOutOfRangeException(nameof(index));

            var bk = ss[index];
            ss.RemoveAt(index);

            SetChange();

            if (bk.Position > 0 && bk.Size > 0) Heap.Free(bk);
        }

        /// <summary>清空列表库</summary>
        public void Clear()
        {
            var ss = Slots;
            if (ss == null || ss.Count == 0) return;

            var arr = ss.ToArray();
            ss.Clear();

            SetChange();

            // 释放空间
            foreach (var bk in arr)
            {
                if (bk.Position > 0 && bk.Size > 0) Heap.Free(bk);
            }
        }
        #endregion

        #region 转列表
        /// <summary>转为列表操作</summary>
        /// <returns></returns>
        public IList<Byte[]> ToList() => new DbList { Db = this };

        private class DbList : IList<Byte[]>
        {
            public ListDb Db { get; set; }

            public Byte[] this[Int32 index] { get => Db.Get(index); set => Db.Set(index, value); }

            public Int32 Count => Db.Count;

            public Boolean IsReadOnly => Db.ReadOnly;

            public void Add(Byte[] item) => Db.Add(item);

            public void Clear() => Db.Clear();

            public Boolean Contains(Byte[] item) => throw new NotSupportedException();

            public void CopyTo(Byte[][] array, Int32 arrayIndex) => throw new NotSupportedException();

            public Int32 IndexOf(Byte[] item) => throw new NotSupportedException();

            public void Insert(Int32 index, Byte[] item) => Db.Insert(index, item);

            public Boolean Remove(Byte[] item) => throw new NotSupportedException();

            public void RemoveAt(Int32 index) => Db.RemoveAt(index);

            public IEnumerator<Byte[]> GetEnumerator()
            {
                for (var i = 0; i < Db.Slots.Count; i++)
                {
                    yield return this[i];
                }
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
        #endregion

        #region 辅助
        /// <summary>日志</summary>
        public ILog Log { get; set; }

        /// <summary>写日志</summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public void WriteLog(String format, params Object[] args) => Log?.Info(format, args);
        #endregion
    }
}