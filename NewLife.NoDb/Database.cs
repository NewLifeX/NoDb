using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NewLife.NoDb.Storage;

namespace NewLife.NoDb
{
    /// <summary>数据库</summary>
    /// <remarks>
    /// 一个Server实例可以有多个数据库。
    /// 每个数据库有自己的头部、索引区和数据区，可对应一个磁盘文件。
    /// </remarks>
    public class Database
    {
        #region 属性
        /// <summary>幻数</summary>
        public String Magic { get; private set; } = "NoDb";

        /// <summary>版本</summary>
        public Int32 Version { get; private set; }

        ///// <summary>数据区堆管理</summary>
        //public Heap Heap { get; private set; }

        private MemoryMappedFile _mmf;
        private MemoryMappedViewAccessor _view;
        #endregion

        #region 构造
        /// <summary>使用数据流实例化数据库</summary>
        /// <param name="stream"></param>
        public Database(Stream stream)
        {
            Read(stream);
        }

        /// <summary>使用内存映射文件实例化数据库</summary>
        /// <param name="file"></param>
        /// <param name="name"></param>
        public Database(String file, String name = null)
        {
            if (name.IsNullOrEmpty())
            {
                if (!file.IsNullOrEmpty())
                    name = Path.GetFileNameWithoutExtension(file.GetFullPath());
                else
                    name = "NoDb";
            }

            if (file.IsNullOrEmpty())
                _mmf = MemoryMappedFile.CreateOrOpen(name, 1024);
            else
                _mmf = MemoryMappedFile.CreateFromFile(file, FileMode.OpenOrCreate, name, 1024);

            _view = _mmf.CreateViewAccessor();

            var fs = _mmf.CreateViewStream();
            var p = fs.Position;
            Read(fs);
            if (Magic != "NoDb")
            {
                fs.Position = p;
                Write(fs);
            }
        }
        #endregion

        #region 方法
        private void Read(Stream stream)
        {
            var reader = new BinaryReader(stream);

            Magic = reader.ReadBytes(4).ToStr();
            Version = reader.ReadByte();
        }

        private void Write(Stream stream)
        {
            var writer = new BinaryWriter(stream);

            writer.Write("NoDb".GetBytes());

            var v = Version;
            if (v <= 0) v = 1;
            writer.Write((Byte)v);
        }
        #endregion
    }
}