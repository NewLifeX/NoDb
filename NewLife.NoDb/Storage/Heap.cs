using System;
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
            // 内存映射未初始化时 mf.Capacity=0
            if (offset < 0 || offset >= mf.Capacity && mf.Capacity > 0) throw new ArgumentOutOfRangeException(nameof(offset));
            if (size < 0) size = mf.Capacity - offset;

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
        #endregion

        #region 基础方法
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
                mb.Size = Size - 8;
                mb.Free = true;
                mb.Write(vw);

                WriteRoot(mb.Position);
            }

            _Free = mb;
        }

        void WriteRoot(Int64 ptr)
        {
            View.Write(0, ptr);
        }

        /// <summary>设置前一块的Next指针，主要考虑头部_Free</summary>
        /// <param name="prev"></param>
        /// <param name="next"></param>
        void SetNextOfPrev(MemoryBlock prev, Int64 next)
        {
            var vw = View;
            // prev为空说明内存分配位于第一空闲块，需要移动_Free
            if (prev == null)
            {
                _Free = new MemoryBlock { Position = next };
                _Free.Read(vw);
                WriteRoot(next);
            }
            else
            {
                prev.Next = next;
                prev.Write(vw);
            }
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
            if ((len & 0b0000_0111) > 0) len = (len & 0b1111_1000) + 8;

            // 同步长度
            len += 8;

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
                    SetNextOfPrev(prev, mb.Next);
                }
                else
                {
                    mb.Write(vw);

                    // 前一块Next指向新切割出来的空闲块
                    SetNextOfPrev(prev, mb.Position);
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
    }
}