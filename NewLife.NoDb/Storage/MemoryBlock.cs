using System;
using NewLife.NoDb.IO;

namespace NewLife.NoDb.Storage
{
    /*
     * 内存块结构：
     * 1，已用块：8字节长度（含3位标识）+ 数据
     * 2，空闲块：8字节长度（含3位标识）+ 8下一空闲指针 + 填充数据 + 8字节长度（含3位标识）
     * 
     * 其中3位标识符：
     * 1，已用块，0位标识上一块是否空闲块
     * 2，空闲块，0位标识本块是否空闲块
     * 
     * 结构要求：
     * 1，申请时，能够顺序找到各空闲块
     * 2，释放时，能够判断前后是否空闲块，从而合并
     * 3，空闲块的前后不可能是其它空闲块
     * 因此，已用块会向前向后查找并判断是否空闲块，空闲块前后都得有长度和标识。
     * 
     * 已用块释放流程：
     * 1，后合并，当前块末边界读8字节长度，如果是空闲块，合并长度
     * 2，前合并，当前块头部前移8字节并读取长度，如果是空闲块，修正位置并合并长度
     */

    /// <summary>内存块</summary>
    public class MemoryBlock
    {
        #region 属性
        /// <summary>位置</summary>
        public Int64 Position { get; set; } = -1;

        /// <summary>大小。包含头尾</summary>
        public Int64 Size { get; set; }

        /// <summary>空闲</summary>
        public Boolean Free { get; set; }

        /// <summary>下一空闲块</summary>
        public Int64 Next { get; set; }
        #endregion

        #region 构造
        #endregion

        #region 方法
        /// <summary>读取内存块，自动识别是否空闲</summary>
        /// <param name="view"></param>
        public void Read(MemoryView view)
        {
            var p = Position;
            if (p < 0) throw new ArgumentNullException(nameof(Position));

            // 不管是否空闲块，都是长度开头
            var len = view.ReadInt64(p);
            var flag = len & 0b0000_0111;
            len &= 0b1111_1000;

            Size = len;
            Free = (len & 0b0000_00001) > 0;

            // 如果是空闲块，还要读取下一空闲指针
            if (Free) Next = view.ReadInt64(p + 8);
        }

        /// <summary>写入内存块</summary>
        /// <param name="view"></param>
        public void Write(MemoryView view)
        {
            var p = Position;
            if (p < 0) throw new ArgumentNullException(nameof(Position));

            // 8字节对齐
            var len = Size;
            if ((len & 0b0000_0111) > 0) len = (len & 0b1111_1000) + 8;

            var flag = Free ? 1L : 0L;
            view.Write(p, len | flag);
            // 使用块与空闲块结构不同
            if (Free)
            {
                view.Write(p + 8, Next);
                view.Write(p + len, len | 0x01);
            }
        }

        /// <summary>读取下一块</summary>
        /// <param name="view"></param>
        /// <returns></returns>
        public MemoryBlock ReadNext(MemoryView view)
        {
            var mb = new MemoryBlock { Position = Next };
            if (Next != 0) mb.Read(view);

            return mb;
        }

        /// <summary>移动到下一块</summary>
        /// <param name="view"></param>
        /// <returns></returns>
        public Boolean MoveNext(MemoryView view)
        {
            if (Next == 0) return false;

            Position = Next;
            Read(view);

            return true;
        }
        #endregion

        #region 辅助
        /// <summary>是否空闲</summary>
        /// <param name="flag"></param>
        /// <returns></returns>
        public static Boolean IsFree(Int64 flag) { return (flag & 0x01) == 0x01; }
        #endregion
    }
}