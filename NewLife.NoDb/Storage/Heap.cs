using System;
using System.IO;
using System.Threading;
using NewLife.Log;
using NewLife.NoDb.IO;

namespace NewLife.NoDb.Storage
{
    /// <summary>堆管理</summary>
    public class Heap : DisposeBase
    {
        #region 属性
        /// <summary>访问器</summary>
        public MemoryView View { get; }

        /// <summary>开始位置</summary>
        public Int64 Position { get; }

        /// <summary>总字节数</summary>
        public Int64 Size { get; }

        private Int64 _Used;
        /// <summary>已分配字节数</summary>
        public Int64 Used => _Used;

        private Int64 _Count;
        /// <summary>已分配块数</summary>
        public Int64 Count => _Count;

        /// <summary>空闲块</summary>
        private MemoryBlock _Free;

        private Object SyncRoot = new Object();
        #endregion

        #region 构造
        /// <summary>实例化数据堆</summary>
        /// <param name="mf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <param name="init">自动初始化加载堆</param>
        public Heap(MemoryFile mf, Int64 offset = 0, Int64 size = -1, Boolean init = true)
        {
            if (mf == null) throw new ArgumentNullException(nameof(mf));
            // 内存映射未初始化时 mf.Capacity=0
            if (offset < 0 || offset >= mf.Capacity && mf.Capacity > 0) throw new ArgumentOutOfRangeException(nameof(offset));
            if (size < 0) size = mf.Capacity - offset;

            Position = offset;
            Size = size;
            View = mf.CreateView(offset, size);

            if (init) Init();
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            View.TryDispose();
        }

        /// <summary>堆管理</summary>
        /// <returns></returns>
        public override String ToString()
        {
            var vw = View;
            var mb = _Free;
            if (mb == null) return vw + "";

            return $"[{vw.File}]({vw.Offset}, {vw.Capacity}) Free=[{mb.Position:n0}, {mb.Size:n0}]";
        }
        #endregion

        #region 基础方法
        const Int32 HeaderSize = 64;

        /// <summary>初始化堆。初始化之后才能使用</summary>
        public void Init()
        {
            // 读取配置
            Load();
            WriteLog("加载堆 {0}", this);

            // 初始化空闲链表
            var mb = _Free;
            if (mb.Position < HeaderSize || !mb.Free) Clear();
        }

        /// <summary>重新初始化内存堆为空</summary>
        public void Clear()
        {
            var vw = View;
            var mb = new MemoryBlock
            {
                Position = HeaderSize,
                Size = Align(Size - HeaderSize, false),

                Free = true,
                PrevFree = false
            };

            _Count = 0;
            _Used = 0;
            _Free = mb;

            WriteLog("初始化堆 {0}", this);

            mb.Write(vw);
            Save();
        }

        /// <summary>设置前一块的Next指针，主要考虑头部_Free</summary>
        /// <param name="prev"></param>
        /// <param name="next"></param>
        void SetNextOfPrev(MemoryBlock prev, MemoryBlock next)
        {
            var vw = View;
            // prev为空说明内存分配位于第一空闲块，需要移动_Free
            if (prev == null)
            {
                var fp = _Free.Position;
                _Free = next;
                if (next.Position != fp) Save();
            }
            else
            {
                prev.Next = next.Position;
                prev.Write(vw);
            }
        }
        #endregion

        #region 序列化
        /// <summary>写入参数</summary>
        private void Save()
        {
            var vw = View;
            var mb = _Free;

            vw.Write(0, _Count);
            vw.Write(8, _Used);
            if (mb != null) vw.Write(16, mb.Position);
        }

        /// <summary>读取参数</summary>
        private void Load()
        {
            var vw = View;
            var mb = _Free ?? new MemoryBlock();

            _Count = vw.ReadInt64(0);
            _Used = vw.ReadInt64(8);
            var fp = mb.Position = vw.ReadInt64(16);

            // 首次读取空闲块
            //if (_Free == null && fp >= HeaderSize && fp < vw.Capacity) mb.Read(vw);
            if (fp >= HeaderSize && fp < vw.Capacity) mb.Read(vw);

            _Free = mb;
        }
        #endregion

        #region 核心分配算法
        /// <summary>分配块</summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public Block Alloc(Int64 size)
        {
            // 增加长度，8字节对齐
            var len = Align(8 + size);

            var vw = View;

            // 暂时加锁分配，将来采用多路空闲链来解决并行分配问题
            lock (SyncRoot)
            {
                // 查找合适空闲块
                var mb = _Free;
                MemoryBlock prev = null;
                while (mb.Size < len)
                {
                    prev = mb;
                    if (!mb.MoveNext(vw)) break;
                }
                if (mb.Size < len) throw new Exception("空间不足");

                // 结果
                var rs = new MemoryBlock { Position = mb.Position, Size = len };

                // 空闲块偏移
                mb.Position += len;
                mb.Size -= len;
                // 不足一个空闲块时，加大结果块
                if (mb.Size < 24)
                {
                    rs.Size += mb.Size;
                    mb.Position += mb.Size;
                    mb.Size = 0;

                    // 前一块Next指向下一块
                    SetNextOfPrev(prev, mb.ReadNext(vw));
                }
                else
                {
                    mb.Write(vw);

                    // 前一块Next指向新切割出来的空闲块
                    SetNextOfPrev(prev, mb);
                }

                // 保存结果块
                rs.Write(vw);

                Interlocked.Increment(ref _Count);
                Interlocked.Add(ref _Used, rs.Size);

                return rs.GetData();
            }
        }

        /// <summary>释放块</summary>
        /// <param name="bk"></param>
        public void Free(Block bk)
        {
            var vw = View;
            lock (SyncRoot)
            {
                // 退8字节就是内存块
                var mb = new MemoryBlock
                {
                    Position = bk.Position - 8
                };
                mb.Read(vw);

                if (mb.Free) throw new ArgumentException("空间已经释放");

                var len = mb.Size + 8;
                mb.Free = false;
                mb.Next = 0;

                // 试图合并右边块
                var mb2 = new MemoryBlock
                {
                    Position = mb.Position + mb.Size
                };
                mb2.Read(vw);
                if (mb2.Free)
                {
                    mb.Size += 8 + mb2.Size;
                    mb.Next = mb2.Next;
                }

                // 试图合并左边块
                var p = vw.ReadInt64(mb.Position - 8);
                p &= 0xF8;
                mb2.Position = mb.Position - p - 8;
                mb2.Read(vw);
                if (mb2.Free)
                {
                    if (mb.Next > 0)
                    {
                        if (mb2.Next != mb.Position + mb.Size) throw new InvalidDataException("数据指针错误");
                    }
                    else
                        mb.Next = mb2.Next;

                    mb.Position = mb2.Position;
                    mb.Size += 8 + mb2.Size;
                }

                mb.Write(vw);

                Interlocked.Decrement(ref _Count);
                Interlocked.Add(ref _Used, -len);
            }
        }

        /// <summary>重分配，扩容</summary>
        /// <param name="ptr"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public Block Realloc(Int64 ptr, Int64 size)
        {
            var vw = View;
            lock (SyncRoot)
            {
                // 退8字节就是内存块
                var mb = new MemoryBlock { Position = ptr - 8 };
                mb.Read(vw);

                if (mb.Free) throw new ArgumentException("空间已经释放");

                var bk = mb.GetData();
                if (bk.Size >= size) return bk;

                // 申请新的内存块
                var bk2 = Alloc(Size);

                // 拷贝数据
                var buf = vw.ReadBytes(bk.Position, (Int32)bk.Size);
                vw.WriteBytes(bk2.Position, buf);

                // 释放旧的
                Free(bk);

                return bk2;
            }
        }
        #endregion

        #region 辅助
        /// <summary>8字节对齐</summary>
        /// <param name="len">要对齐的长度</param>
        /// <param name="up">向上对齐。默认true</param>
        /// <returns></returns>
        private static Int64 Align(Int64 len, Boolean up = true)
        {
            // 8字节对齐
            var flag = len & 0b0000_0111;
            if (flag > 0)
            {
                len -= flag;
                if (up) len += 8;
            }

            return len;
        }

        /// <summary>日志</summary>
        public ILog Log { get; set; }

        /// <summary>写日志</summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public void WriteLog(String format, params Object[] args)
        {
            Log?.Info(format, args);
        }
        #endregion
    }
}