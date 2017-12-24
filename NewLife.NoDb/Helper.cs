using System;
using System.IO;
using System.IO.MemoryMappedFiles;
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
            return null;
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
    }
}