using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace NewLife.NoDb.Storage
{
    /// <summary>堆管理</summary>
    public class Heap
    {
        #region 属性
        /// <summary>总字节数</summary>
        public Int64 Total { get; }

        private Int64 _Size;
        /// <summary>已分配字节数</summary>
        public Int64 Size => _Size;

        private Int64 _Count;
        /// <summary>已分配块数</summary>
        public Int64 Count => _Count;

        /// <summary>速度优先。默认true</summary>
        public Boolean SpeedFirst { get; set; } = true;

        /// <summary>当前可用的空闲片</summary>
        private Int32 activeIndex = -1;

        /// <summary>空闲分片。有序</summary>
        private List<Block> free = new List<Block>();
        #endregion

        #region 构造
        /// <summary>实例化数据堆</summary>
        /// <param name="bk"></param>
        public Heap(Block bk)
        {
            // 建立新的头部

            free.Add(bk);
            Total = bk.Size;
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
            var idx = activeIndex;
            // 当前块不存在、已到末尾、大小不足等，都需要重新查找
            if (idx < 0 || idx == free.Count - 1 || free[idx].Size < size)
            {
                var start = 0;
                if (SpeedFirst) start = idx >= 0 && idx + 1 < free.Count - 1 ? idx + 1 : 0;

                // 查找一块足够大的分片
                idx = -1;
                for (var i = start; i < free.Count; i++)
                {
                    if (free[i].Size >= size)
                    {
                        idx = activeIndex = i;
                        break;
                    }
                }
                if (idx < 0) throw new Exception("空间不足");
            }

            var bk = free[idx];
            if (bk.Size < size) throw new Exception("空间不足");

            var pos = bk.Position;
            bk.Position += size;
            bk.Size -= size;

            if (bk.Size == 0)
            {
                //free.RemoveAt(idx);

                // 下一次开始搜索的位置
                activeIndex = -1;
            }

            Interlocked.Increment(ref _Count);
            Interlocked.Add(ref _Size, size);

            return new Block(pos, size);
        }

        /// <summary>释放块</summary>
        /// <param name="bk"></param>
        public void Free(Block bk)
        {
            // 找到刚好比目标块要大的那一块
            var idx = -1;
            for (var i = 0; i < free.Count; i++)
            {
                if (free[i].Position > bk.Position)
                {
                    idx = i;
                    break;
                }
            }
            if (idx < 0) throw new ArgumentException("非法释放");
            if (free[idx].Position == bk.Position) throw new ArgumentException("空间已经释放");

            if ((idx < free.Count && bk.Next > free[idx].Position) || (idx > 0 && bk.Position < free[idx - 1].Next))
                throw new ArgumentException("不能释放重叠空间");

            var merged = false;
            // 试图合并右边块
            if (idx < free.Count)
            {
                var p = free[idx];
                if (bk.Next == p.Position)
                {
                    p.Position -= bk.Size;
                    p.Size += bk.Size;
                    // 已合并
                    merged = true;
                }
            }

            // 试图合并左边块
            if (idx > 0)
            {
                var p = free[idx - 1];
                if (bk.Position == p.Next)
                {
                    if (merged)
                    {
                        p.Size += free[idx].Size;
                        free.RemoveAt(idx);
                        if (activeIndex >= idx) activeIndex--;
                    }
                    else
                    {
                        p.Size += bk.Size;
                        merged = true;
                    }
                }
            }

            if (!merged) free.Insert(idx, bk);

            Interlocked.Decrement(ref _Count);
            Interlocked.Add(ref _Size, -bk.Size);
        }
        #endregion
    }
}