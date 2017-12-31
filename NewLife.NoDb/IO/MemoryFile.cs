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

        /// <summary>映射</summary>
        public MemoryMappedFile Map { get; private set; }

        /// <summary>容量</summary>
        public Int64 Capacity { get; private set; }

        private Object SyncRoot = new Object();
        #endregion

        #region 构造
        /// <summary>实例化内存映射文件</summary>
        /// <param name="file"></param>
        public MemoryFile(String file)
        {
            FileName = file;

            if (!file.IsNullOrEmpty()) Name = Path.GetFileNameWithoutExtension(file);

#if DEBUG
            Log = XTrace.Log;
#endif
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            Map.TryDispose();
            Map = null;

            Stream.TryDispose();
            Stream = null;
        }
        #endregion

        #region 核心方法
        private void Init()
        {
            if (Map != null) return;

            CheckCapacity(1024);
        }

        /// <summary>检查容量并扩容</summary>
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

                WriteLog("扩容到 {0:n0}", capacity);

                // 先销毁旧的
                Map.TryDispose();
                Stream.TryDispose();

                var mapName = "MMF_" + Name;

                // 带文件和不带文件
                if (FileName.IsNullOrEmpty())
                {
                    Stream = null;
                    Capacity = capacity;
                    Map = MemoryMappedFile.CreateOrOpen(mapName, capacity, MemoryMappedFileAccess.ReadWrite, MemoryMappedFileOptions.DelayAllocatePages, null, HandleInheritability.None);
                }
                else
                {
                    // 使用文件流可以控制共享读写，让别的进程也可以读写文件
                    var fs = new FileStream(FileName.GetFullPath(), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.RandomAccess);
                    if (fs.Length < capacity) fs.SetLength(capacity);

                    Stream = fs;
                    Capacity = fs.Length;

                    // 最大容量为0表示使用文件流最大值
                    Map = MemoryMappedFile.CreateFromFile(fs, mapName, 0, MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, true);
                }
            }

            return true;
        }

        /// <summary>创建视图</summary>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public MemoryView CreateView(Int64 offset = 0, Int64 size = 0)
        {
            //Init();
            CheckCapacity(offset + size);

            return new MemoryView(this, offset, size);
        }
        #endregion

        #region 日志
        /// <summary>日志</summary>
        public ILog Log { get; set; }

        /// <summary>写日志</summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public void WriteLog(String format, params Object[] args)
        {
            Log?.Info("[" + Name + "]" + format, args);
        }
        #endregion
    }
}