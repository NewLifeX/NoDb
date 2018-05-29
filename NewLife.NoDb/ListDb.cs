using System;
using System.Collections.Generic;
using NewLife.NoDb.Collections;
using NewLife.NoDb.IO;
using NewLife.NoDb.Storage;

namespace NewLife.NoDb
{
    /// <summary>列表数据库</summary>
    /// <remarks>
    /// 以顺序整数为键，例如，以秒数为键按天分库，存储时序数据
    /// </remarks>
    public class ListDb
    {
        #region 属性
        /// <summary>幻数</summary>
        public const String Magic = "NoDb";

        /// <summary>映射文件</summary>
        public MemoryFile File { get; }

        /// <summary>版本</summary>
        public Int32 Version { get; private set; } = 1;

        /// <summary>顺序整数构成的索引</summary>
        public MemoryArray<Int64> Index { get; private set; }

        /// <summary>元素个数</summary>
        public Int32 Count { get; private set; }

        /// <summary>数据区</summary>
        public Heap Heap { get; private set; }

        /// <summary>访问器</summary>
        private MemoryView View { get; }
        #endregion

        #region 构造
        /// <summary>实例化数据库</summary>
        /// <param name="file">文件</param>
        /// <param name="count">列表个数。默认0表示只读，否则从文件读取个数</param>
        public ListDb(String file, Int32 count = 0)
        {
            File = new MemoryFile(file);

            // 加载
            if (!Read())
            {
                if (count == 0) throw new InvalidOperationException("无效数据库！");

                Write(count);
            }

            //Index = new MemoryArray<Int32>(count, File);
            View = File.CreateView();
        }
        #endregion

        #region 读写
        private Boolean Read()
        {
            var vw = File.CreateView(0, 1024);

            var magic = vw.ReadBytes(0, 4).ToStr();
            if (Magic != magic) return false;

            // 版本
            Version = vw.ReadInt32(4);

            // 索引器位置和个数
            var offset = vw.ReadInt32(8);
            var size = vw.ReadInt32(12);
            Count = size / sizeof(Int64);

            Index = new MemoryArray<Int64>(Count, File, offset);

            // 数据区
            offset = vw.ReadInt32(16);
            Heap = new Heap(File, offset);

            return true;
        }

        private void Write(Int32 count)
        {
            var vw = File.CreateView(0, 1024);
            vw.WriteBytes(0, Magic.GetBytes());

            // 版本
            vw.Write(4, Version);

            // 索引器
            var offset = 64;
            var size = count * sizeof(Int64);
            if (Index != null)
            {
                offset = (Int32)Index.View.Offset;
                size = (Int32)Index.View.Size;
            }
            vw.Write(8, offset);
            vw.Write(12, size);

            // 数据区
            offset = 0;
            if (Heap != null) offset = (Int32)Heap.Position;
            vw.Write(16, offset);

            Count = count;
        }
        #endregion

        #region 索引器
        /// <summary>获取 或 设置 指定键关联的值</summary>
        /// <param name="idx"></param>
        /// <returns></returns>
        public Block this[Int32 idx]
        {
            get
            {
                if (idx < 0 || idx >= Count) throw new KeyNotFoundException();

                var n = Index[idx];
                var bk = new Block(n >> 32, n & 0xFFFF_FFFF);

                return bk;
            }
            set
            {
                if (idx < 0 || idx >= Count) throw new KeyNotFoundException();
                if (value.Position > 0xFFFF_FFFF || value.Size > 0xFFFF_FFFF) throw new ArgumentOutOfRangeException();

                //ref var bk = ref value;

                var n = (value.Position << 32) + value.Size;
                Index[idx] = n;
            }
        }
        #endregion

        #region 取值 / 设置
        /// <summary>获取 数据</summary>
        /// <param name="idx"></param>
        /// <returns></returns>
        public Byte[] Get(Int32 idx)
        {
            var bk = this[idx];
            if (bk.Position == 0 || bk.Size == 0) return null;

            return View.ReadBytes(bk.Position, (Int32)bk.Size);
        }

        /// <summary>设置 数据</summary>
        /// <param name="idx"></param>
        /// <param name="value"></param>
        public void Set(Int32 idx, Byte[] value)
        {
            var bk = this[idx];
            if (bk.Position == 0 || bk.Size == 0) bk = Heap.Alloc(value.Length);

            View.WriteBytes(bk.Position, value);

            this[idx] = bk;
        }
        #endregion
    }
}