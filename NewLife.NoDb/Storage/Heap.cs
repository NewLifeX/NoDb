using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
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

        ///// <summary>速度优先。默认true</summary>
        //public Boolean SpeedFirst { get; set; } = true;

        ///// <summary>当前可用的空闲片</summary>
        //private Int32 activeIndex = -1;

        /// <summary>空闲块</summary>
        private MemoryBlock _Free;

        private Object SyncRoot = new Object();
        #endregion

        #region 构造
        /// <summary>实例化数据堆</summary>
        /// <param name="mf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        public Heap(MemoryFile mf, Int64 offset = 0, Int64 size = -1)
        {
            if (mf == null) throw new ArgumentNullException(nameof(mf));
            if (offset < 0 || offset >= mf.Capacity && mf.Capacity > 0) throw new ArgumentOutOfRangeException(nameof(offset));
            if (size < 0) size = mf.Capacity - offset;

            //free = freeList ?? new List<Block>();

            //free.Add(bk);

            Position = offset;
            Size = size;
            View = mf.CreateView(offset, size);

            Init(new Block(offset, size));
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            View.TryDispose();
        }

        void Init(Block bk)
        {
            var vw = View;
            var mb = new MemoryBlock();

            // 空闲块指针
            var fp = vw.ReadInt64(0);
            if (fp >= 0 && fp < vw.Capacity)
            {
                // 读取第一个空闲块
                mb.Position = fp;
                mb.Read(vw);
            }

            // 初始化空闲链表
            if (mb.Size == 0)
            {
                mb.Position = 8;
                mb.Size = Size - 8 - 8;
                mb.Free = true;
                mb.Write(vw);

                vw.Write(0, mb.Position);
            }

            _Free = mb;
        }
        #endregion

        #region 序列化
        private void Serialize(BinaryWriter writer) { }

        private void Deserialize(BinaryReader reader) { }
        #endregion

        #region 核心分配算法
        /// <summary>分配块</summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public Block Alloc(Int64 size)
        {
            // 8字节对齐
            var len = size;
            if ((len & 0x07) > 0) len = (len & 0xF8) + 8;

            // 同步长度
            len += 8;

            var mb = _Free;
            while (mb != null && mb.Size < len)
            {
                if (mb.Next == 0) break;

                mb = mb.ReadNext(View);
            }
            if (mb == null || len > mb.Size) throw new Exception("空间不足");
            lock (SyncRoot)
            {
                mb = _Free;
                while (mb != null && mb.Size < len)
                {
                    if (mb.Next == 0) break;

                    mb = mb.ReadNext(View);
                }
                if (mb == null || len > mb.Size) throw new Exception("空间不足");

                // 结果
                Block bk;
                bk.Position = mb.Position + 8;
                bk.Size = len - 8;

                // 写入使用块长度
                View.Write(mb.Position, bk.Size);

                // 空闲块偏移
                mb.Position += len;
                mb.Size -= len;
                mb.Write(View);

                // 空闲块开始地址改变
                if (mb == _Free)
                {
                    View.Write(0, mb.Position);
                }

                Interlocked.Increment(ref _Count);
                Interlocked.Add(ref _Used, len);

                return bk;
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
                var mb = new MemoryBlock();
                mb.Position = bk.Position - 8;
                mb.Read(vw);

                if (mb.Free) throw new ArgumentException("空间已经释放");

                var len = mb.Size + 8;
                mb.Free = false;
                mb.Next = 0;

                // 试图合并右边块
                var mb2 = new MemoryBlock();
                mb2.Position = mb.Position + mb.Size;
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
        #endregion
    }
}