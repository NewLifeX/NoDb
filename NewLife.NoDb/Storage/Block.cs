using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

namespace NewLife.NoDb.Storage
{
    /// <summary>数据块</summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct Block : IEquatable<Block>, IComparable<Block>
    {
        #region  属性
        /// <summary>空数据库</summary>
        public static readonly Block Null = new Block(0, 0);

        /// <summary>位置</summary>
        public Int64 Position;

        /// <summary>大小</summary>
        public Int64 Size;
        #endregion

        #region 构造
        /// <summary>实例化</summary>
        /// <param name="position"></param>
        /// <param name="size"></param>
        [DebuggerStepThrough]
        public Block(Int64 position, Int64 size)
        {
            Position = position;
            Size = size;
        }
        #endregion

        #region 方法
        /// <summary>数据块是否为空</summary>
        public Boolean IsNull => Position == 0 && Size == 0;

        /// <summary>
        /// Returns index of the block after fragment.
        /// </summary>
        public Int64 PositionPlusSize => checked(Position + Size);

        /// <summary>序列化</summary>
        /// <param name="writer"></param>
        public void Write(BinaryWriter writer)
        {
            writer.Write(Position);
            writer.Write(Size);
        }

        /// <summary>反序列化</summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static Block Read(BinaryReader reader)
        {
            var position = reader.ReadInt64();
            var size = reader.ReadInt64();

            return new Block(position, size);
        }

        /// <summary>是否包含</summary>
        /// <param name="position"></param>
        /// <returns></returns>
        public Boolean Contains(Int64 position) => Position <= position && position < Position + Size;
        #endregion

        #region 相等比较
        /// <summary>相等</summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public Boolean Equals(Block other) => Position == other.Position && Size == other.Size;

        /// <summary>比较</summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public Int32 CompareTo(Block other) => Position.CompareTo(other.Position);

        /// <summary>相等/summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override Boolean Equals(Object obj)
        {
            if (obj is Block) return Equals((Block)obj);

            return false;
        }

        /// <summary>哈希</summary>
        /// <returns></returns>
        public override Int32 GetHashCode() => Position.GetHashCode() ^ Size.GetHashCode();

        /// <summary>字符串</summary>
        /// <returns></returns>
        public override String ToString() => String.Format("({0}, {1})", Position, Size);

        /// <summary>等于</summary>
        /// <param name="block1"></param>
        /// <param name="block2"></param>
        /// <returns></returns>
        public static Boolean operator ==(Block block1, Block block2) => block1.Equals(block2);

        /// <summary>不等于</summary>
        /// <param name="block1"></param>
        /// <param name="block2"></param>
        /// <returns></returns>
        public static Boolean operator !=(Block block1, Block block2) => !(block1 == block2);

        /// <summary>重载加号</summary>
        /// <param name="block"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        public static Block operator +(Block block, Int64 offset) => new Block(block.Position + offset, block.Size);
        #endregion
    }
}