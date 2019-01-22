using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using NewLife.NoDb.Storage;

namespace NewLife.NoDb
{
    /// <summary>帮助类</summary>
    public static class Helper
    {
        /// <summary></summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static MemoryMappedFile CreateFromFile(String file)
        {
            file = file.GetFullPath();
            var name = Path.GetFileNameWithoutExtension(file);

            //var capacity = 4 * 1024 * 1024;
            //if (file.AsFile().Exists) capacity = 0;
            //_mmf = MemoryMappedFile.CreateFromFile(file, FileMode.OpenOrCreate, name, capacity, MemoryMappedFileAccess.ReadWrite);

            // 使用文件流可以控制共享读写，让别的进程也可以读写文件
            var fs = new FileStream(file, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.RandomAccess);
            if (fs.Length == 0) fs.SetLength(1024);
#if __CORE__
            var _mmf = MemoryMappedFile.CreateFromFile(fs, name, 0, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false);
#else
            var _mmf = MemoryMappedFile.CreateFromFile(fs, name, 0, MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, false);
#endif

            return _mmf;
        }

        /// <summary></summary>
        /// <param name="mmf"></param>
        /// <param name="block"></param>
        /// <returns></returns>
        public static Stream CreateStream(this MemoryMappedFile mmf, Block block)
        {
            return mmf.CreateViewStream(block.Position, block.Size);
        }

        /// <summary></summary>
        /// <param name="mmf"></param>
        /// <param name="block"></param>
        /// <returns></returns>
        public static UnmanagedMemoryAccessor CreateAccessor(this MemoryMappedFile mmf, Block block)
        {
            return mmf.CreateViewAccessor(block.Position, block.Size);
        }

        /// <summary></summary>
        /// <param name="accessor"></param>
        /// <param name="position"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static Byte[] ReadArray(this UnmanagedMemoryAccessor accessor, Int32 position, Int32 count)
        {
            var buf = new Byte[count];
            var n = accessor.ReadArray(position, buf, 0, buf.Length);
            if (n <= buf.Length) buf = buf.ReadBytes(0, n);

            return buf;
        }

        /// <summary></summary>
        /// <param name="accessor"></param>
        /// <param name="position"></param>
        /// <returns></returns>
        public static Block ReadBlock(this UnmanagedMemoryAccessor accessor, Int32 position)
        {
            return Block.Null;
        }

        ///// <summary></summary>
        ///// <param name="mmf"></param>
        //public static void CheckAccessControl(this MemoryMappedFile mmf)
        //{
        //    var user = $"{Environment.MachineName}\\{Environment.UserName}";
        //    var rule = new AccessRule<MemoryMappedFileRights>(user, MemoryMappedFileRights.FullControl, AccessControlType.Allow);

        //    var msc = mmf.GetAccessControl();
        //    foreach (AccessRule<MemoryMappedFileRights> ar in msc.GetAccessRules(true, true, typeof(NTAccount)))
        //    {
        //        if (ar.IdentityReference == rule.IdentityReference && ar.AccessControlType == rule.AccessControlType && ar.Rights == rule.Rights) return;
        //    }

        //    msc.AddAccessRule(rule);
        //    mmf.SetAccessControl(msc);
        //}

        //public static unsafe Byte[] ReadBytes(this MemoryMappedViewAccessor view, Int64 offset, Int32 num)
        //{
        //    var ptr = (Byte*)0;
        //    view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

        //    var p = new IntPtr(ptr);
        //    p = new IntPtr(p.ToInt64() + offset);
        //    var arr = new Byte[num];
        //    Marshal.Copy(p, arr, 0, num);

        //    view.SafeMemoryMappedViewHandle.ReleasePointer();
        //    return arr;

        //    //var arr = new Byte[num];
        //    //view.ReadArray(offset, arr, 0, num);

        //    //return arr;
        //}

        //public static unsafe void WriteBytes(this MemoryMappedViewAccessor accessor, Int64 offset, Byte[] data)
        //{
        //    var ptr = (Byte*)0;
        //    accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

        //    var p = new IntPtr(ptr);
        //    p = new IntPtr(p.ToInt64() + offset);
        //    Marshal.Copy(data, 0, p, data.Length);

        //    accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        //}
    }
}