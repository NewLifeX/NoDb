using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;
using NewLife.NoDb.Storage;

namespace NewLife.NoDb
{
    /// <summary>帮助类</summary>
    static class Helper
    {
        public static Stream CreateStream(this MemoryMappedFile mmf, Block block)
        {
            return mmf.CreateViewStream(block.Position, block.Size);
        }

        public static UnmanagedMemoryAccessor CreateAccessor(this MemoryMappedFile mmf, Block block)
        {
            return mmf.CreateViewAccessor(block.Position, block.Size);
        }

        public static Byte[] ReadArray(this UnmanagedMemoryAccessor accessor, Int32 position, Int32 count)
        {
            var buf = new Byte[count];
            var n = accessor.ReadArray(position, buf, 0, buf.Length);
            if (n <= buf.Length) buf = buf.ReadBytes(0, n);

            return buf;
        }

        public static Block ReadBlock(this UnmanagedMemoryAccessor accessor, Int32 position)
        {
            return Block.Null;
        }

        public static void CheckAccessControl(this MemoryMappedFile mmf)
        {
            var user = $"{Environment.MachineName}\\{Environment.UserName}";
            var rule = new AccessRule<MemoryMappedFileRights>(user, MemoryMappedFileRights.FullControl, AccessControlType.Allow);

            var msc = mmf.GetAccessControl();
            foreach (AccessRule<MemoryMappedFileRights> ar in msc.GetAccessRules(true, true, typeof(NTAccount)))
            {
                if (ar.IdentityReference == rule.IdentityReference && ar.AccessControlType == rule.AccessControlType && ar.Rights == rule.Rights) return;
            }

            msc.AddAccessRule(rule);
            mmf.SetAccessControl(msc);
        }

        public static unsafe Byte[] ReadBytes(this MemoryMappedViewAccessor view, Int64 offset, Int32 num)
        {
            var ptr = (Byte*)0;
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

            var p = new IntPtr(ptr);
            p = new IntPtr(p.ToInt64() + offset);
            var arr = new Byte[num];
            Marshal.Copy(p, arr, 0, num);

            view.SafeMemoryMappedViewHandle.ReleasePointer();
            return arr;

            //var arr = new Byte[num];
            //view.ReadArray(offset, arr, 0, num);

            //return arr;
        }

        public static unsafe void WriteBytes(this MemoryMappedViewAccessor accessor, Int64 offset, Byte[] data)
        {
            var ptr = (Byte*)0;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);

            var p = new IntPtr(ptr);
            p = new IntPtr(p.ToInt64() + offset);
            Marshal.Copy(data, 0, p, data.Length);

            accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }
}