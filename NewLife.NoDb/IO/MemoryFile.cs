using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using NewLife.Log;

namespace NewLife.NoDb.IO
{
    /// <summary>内存映射文件</summary>
    public class MemoryFile : DisposeBase
    {
        #region 属性
        /// <summary>映射名称</summary>
        public String Name { get; set; }

        /// <summary>文件</summary>
        public String FileName { get; }

        /// <summary>文件流</summary>
        public FileStream Stream { get; private set; }

        /// <summary>映射。每次扩容时重新实例化</summary>
        public MemoryMappedFile Map { get; private set; }

        /// <summary>容量。默认0，首次使用初始化</summary>
        public Int64 Capacity { get; private set; }

        /// <summary>版本</summary>
        public Int32 Version { get; private set; }

        /// <summary>只读</summary>
        public Boolean Readonly { get; private set; }

        private readonly Object SyncRoot = new Object();
        #endregion

        #region 构造
        /// <summary>实例化内存映射文件</summary>
        /// <param name="file">文件</param>
        /// <param name="readOnly">只读。默认false</param>
        public MemoryFile(String file, Boolean readOnly = false)
        {
            FileName = file;

            if (!file.IsNullOrEmpty()) Name = Path.GetFileNameWithoutExtension(file);

            Readonly = readOnly;

#if DEBUG
            Log = XTrace.Log;
#endif
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void Dispose(Boolean disposing)
        {
            base.Dispose(disposing);

            Map.TryDispose();
            Map = null;

            Stream.TryDispose();
            Stream = null;
        }

        /// <summary>映射文件名字</summary>
        /// <returns></returns>
        public override String ToString() => Name;
        #endregion

        #region 核心方法
        private void Init()
        {
            if (Map != null) return;

            CheckCapacity(1024);
        }

        /// <summary>检查容量并扩容。扩容时其它线程使用映射内存有风险</summary>
        /// <param name="capacity">期望达到的目标容量</param>
        /// <returns></returns>
        public Boolean CheckCapacity(Int64 capacity)
        {
            // 容量凑够8字节对齐
            var n = capacity % 8;
            if (n > 0) capacity += 8 - n;

            if (capacity <= Capacity) return false;
            lock (SyncRoot)
            {
                if (capacity <= Capacity) return false;

                //WriteLog("扩容到 {0:n0}", capacity);

                // 先销毁旧的
                Map.TryDispose();
                //Stream.TryDispose();

                var mapName = "MMF_" + Name;
                var access = Readonly ? MemoryMappedFileAccess.Read : MemoryMappedFileAccess.ReadWrite;

                // 不带文件的纯内存映射
                if (FileName.IsNullOrEmpty())
                {
                    //Stream = null;
                    Capacity = capacity;
#if __CORE__
                    Map = MemoryMappedFile.CreateOrOpen(mapName, capacity, access, MemoryMappedFileOptions.DelayAllocatePages, HandleInheritability.None);
#else
                    Map = MemoryMappedFile.CreateOrOpen(mapName, capacity, access, MemoryMappedFileOptions.DelayAllocatePages, null, HandleInheritability.None);
#endif
                }
                else
                {
                    // 使用文件流可以控制共享读写，让别的进程也可以读写文件
                    var fs = Stream;
                    if (Readonly)
                    {
                        if (fs == null) fs = new FileStream(FileName.GetFullPath(), FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.RandomAccess);
                        if (fs.Length < capacity) throw new InvalidDataException($"文件长度不足 {capacity}");
                    }
                    else
                    {
                        if (fs == null) fs = new FileStream(FileName.GetFullPath(), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.RandomAccess);
                        if (fs.Length < capacity) fs.SetLength(capacity);
                    }

                    Stream = fs;
                    Capacity = fs.Length;

                    // 最大容量为0表示使用文件流最大值
#if __CORE__
                    Map = MemoryMappedFile.CreateFromFile(fs, mapName, 0, access, HandleInheritability.None, true);
#else
                    Map = MemoryMappedFile.CreateFromFile(fs, mapName, 0, access, null, HandleInheritability.None, true);
#endif
                }

                //Interlocked.Increment(ref _Version);
                Version++;
            }

            return true;
        }

        /// <summary>创建视图</summary>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public MemoryView CreateView(Int64 offset = 0, Int64 size = 0) =>
            //CheckCapacity(offset + size);

            new MemoryView(this, offset, size);
        #endregion

        #region 日志
        /// <summary>日志</summary>
        public ILog Log { get; set; }

        /// <summary>写日志</summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public void WriteLog(String format, params Object[] args) => Log?.Info("[" + Name + "]" + format, args);
        #endregion
    }
}