using System;
using System.IO;
using NewLife.Data;
using NewLife.NoDb.IO;
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
        public const String Magic = "NoDb";

        /// <summary>映射文件</summary>
        public MemoryFile File { get; }

        /// <summary>版本</summary>
        public Int32 Version { get; private set; }

        /// <summary>索引区</summary>
        private DbIndex Index { get; set; }

        ///// <summary>数据区</summary>
        //public Heap Heap { get; private set; }
        #endregion

        #region 构造
        /// <summary>使用内存映射文件实例化数据库</summary>
        /// <param name="file"></param>
        public Database(String file) => File = new MemoryFile(file);
        #endregion

        #region 主要方法
        /// <summary>获取</summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public Packet Get(String key) =>
            /*
* 1，从索引区找到节点信息
* 2，根据节点信息指向，从数据区读取数据
*/

            //if (!TryGetValue(key, out var block)) return null;

            null;

        ///// <summary>尝试获取</summary>
        ///// <param name="key"></param>
        ///// <param name="block"></param>
        ///// <returns></returns>
        //public Boolean TryGetValue(String key, out Block block)
        //{
        //    //block = null;
        //    return false;
        //}

        /// <summary>设置</summary>
        /// <param name="key"></param>
        /// <param name="pk"></param>
        /// <returns></returns>
        public Boolean Set(String key, Packet pk) => false;
        #endregion

        #region 序列化
        private Boolean Read(Stream stream)
        {
            if (stream.Length < 6) return false;

            var magic = stream.ReadBytes(4).ToStr();
            if (Magic != magic) return false;

            // 版本和头部长度
            Version = stream.ReadByte();
            var len = stream.ReadByte();
            if (len < 32 + 4 || stream.Position + len >= stream.Length) return false;

            var buf = stream.ReadBytes(len);
            var ms = new MemoryStream(buf);
            var reader = new BinaryReader(ms);

            var idx = Block.Read(reader);
            var data = Block.Read(reader);

            len = (Int32)ms.Position;
            var crc = reader.ReadUInt32();

            // 校验
            if (crc != buf.ReadBytes(0, len).Crc()) return false;

            //// 索引区
            //Index = new DbIndex(_mmf, idx);

            // 数据区
            //Heap = new Heap(_mmf, data);

            return true;
        }

        private void Write(Stream stream)
        {
            var writer = new BinaryWriter(stream);

            writer.Write(Magic.GetBytes());

            var v = Version;
            if (v <= 0) v = 1;
            writer.Write((Byte)v);

            //writer.Write((Byte)32);
            var ms = new MemoryStream();
            var writer2 = new BinaryWriter(ms);

            //// 索引区
            //var idx = new Block(256, 2 * 1024 * 1024);
            //Index = new DbIndex(_mmf, idx);
            //idx.Write(writer2);

            //// 数据区
            //bk = Heap.GetArea();
            //bk.Write(writer2);

            // 计算校验
            var buf = ms.ToArray();
            var crc = buf.Crc();
            writer.Write((Byte)32);
            writer.Write(buf, 0, buf.Length);
            writer.Write(crc);
        }
        #endregion
    }
}