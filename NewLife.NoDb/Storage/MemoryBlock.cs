using System;
using NewLife.NoDb.IO;

namespace NewLife.NoDb.Storage
{
    /*
     * 内存块结构：
     * 1，已用块：8字节长度（含3位标识）+数据，其中长度低3位为标记位，最低位表示上一节是否空闲块
     * 2，空闲块：8字节长度（含3位标识）+8下一空闲指针+填充数据+8字节长度
     */

    /// <summary>内存块</summary>
    public class MemoryBlock
    {
        #region 属性
        /// <summary>位置</summary>
        public Int64 Position { get; set; }

        /// <summary>大小</summary>
        public Int64 Size { get; set; }

        /// <summary>空闲</summary>
        public Boolean Free { get; set; }

        /// <summary>下一空闲块</summary>
        public Int64 Next { get; set; }

        #endregion

        #region 构造
        ///// <summary>实例化一个内存块</summary>
        ///// <param name="mmf"></param>
        ///// <param name="offset"></param>
        ///// <param name="size"></param>
        //public MemoryBlock(MemoryMappedFile mmf, Int64 offset, Int32 size)
        //{
        //    _mmf = mmf;
        //    Offset = offset;
        //    Size = size;
        //}
        #endregion

        #region 方法
        /// <summary>读取内存块</summary>
        /// <param name="view"></param>
        public void Read(MemoryView view)
        {
            var p = Position;

            var len = view.ReadInt64(p);
            var flag = len & 0b0000_0111;
            len &= 0b1111_1000;

            Size = len;
            Free = (len & 0x01) == 0x01;

            if (Free) Next = view.ReadInt64(p + 8);
        }

        /// <summary>写入内存块</summary>
        /// <param name="view"></param>
        public void Write(MemoryView view)
        {
            var p = Position;

            var len = view.ReadInt64(p);
            var flag = len & 0b0000_0111;

            // 8字节对齐
            len = Size;
            if ((len & 0x07) > 0) len = (len & 0xF8) + 8;

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
            var mb = new MemoryBlock();
            mb.Position = Next;
            if (Next != 0) mb.Read(view);

            return mb;
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