using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewLife.NoDb.Storage
{
    /// <summary>内存块</summary>
    public class MemoryBlock
    {
        #region 属性
        private MemoryMappedFile _mmf;

        /// <summary>偏移</summary>
        public Int64 Offset { get; private set; }

        /// <summary>大小</summary>
        public Int32 Size { get; private set; }
        #endregion

        #region 构造
        /// <summary>实例化一个内存块</summary>
        /// <param name="mmf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        public MemoryBlock(MemoryMappedFile mmf, Int64 offset, Int32 size)
        {
            _mmf = mmf;
            Offset = offset;
            Size = size;
        }
        #endregion

        #region 方法
        #endregion
    }
}